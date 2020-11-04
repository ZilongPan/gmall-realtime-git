package com.sleepy.gmall.realtime.app.dws

import java.lang
import java.util.Properties

import com.sleepy.gmall.realtime.app.BaseApp
import com.sleepy.gmall.realtime.app.dwd.fact.{OrderDetailApp, OrderInfoApp}
import com.sleepy.gmall.realtime.bean.dwd.fact.{OrderDetail, OrderInfo}
import com.sleepy.gmall.realtime.bean.dws.OrderWide
import com.sleepy.gmall.realtime.util._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * Desc: 从Kafka的DWD层，读取订单和订单明细数据
 */
object OrderWideApp extends BaseApp[OrderWide] {
    //订单- 主题和消费者组
    val orderInfoTopic = "dwd_order_info"
    val orderInfoGroupId = "dws_order_info_group"
    //订单详情- 主题和消费者组
    val orderDetailTopic = "dwd_order_detail"
    val orderDetailGroupId = "dws_order_detail_group"

    //窗口大小
    val windowDuration: Duration = Seconds(20)
    //步长
    val slideDuration: Duration = Seconds(5)

    def start(): Unit = {
        defaultStart()
    }

    def defaultStart(): Unit = {
        //===============1.从Kafka中获取数据===============
        //转换为kv结构
        //开窗
        val orderInfoDS: DStream[(Long, OrderInfo)] = OrderInfoApp.getData(orderInfoTopic, orderInfoGroupId)
          .map(orderInfo => {
              (orderInfo.id, orderInfo)
          }).window(windowDuration, slideDuration)
        val orderDetailDS: DStream[(Long, OrderDetail)] = OrderDetailApp.getData(orderDetailTopic, orderDetailGroupId)
          .map(orderDetail => {
              (orderDetail.order_id, orderDetail)
          }).window(windowDuration, slideDuration)

        //===============2.双流Join================
        //双流join
        val OrderWideDStream: DStream[OrderWide] = orderInfoDS.join(orderDetailDS)
          //去重  Redis   type:set    key: order_join:[orderId]   value:orderDetailId  expire :600
          .mapPartitions(tupleItr => {
              val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
              //获取Jedis客户端
              val jedis: Jedis = RedisUtil.getJedisClient()
              val orderWideList = new ListBuffer[OrderWide]
              tupleList.foreach {
                  case (orderId, (orderInfo, orderDetail)) => {
                      val orderKey: String = "order_join:" + orderId
                      val isNotExists: lang.Long = jedis.sadd(orderKey, orderDetail.id.toString)
                      jedis.expire(orderKey, 600)
                      if (isNotExists == 1L) {
                          orderWideList.append(new OrderWide(orderInfo, orderDetail))
                      }
                  }
              }
              jedis.close()
              orderWideList.toIterator
          })
          //orderWideDStream.print(1000)

          //===============3.实付分摊================
          .mapPartitions(orderWideItr => {
              val orderWideList: List[OrderWide] = orderWideItr.toList
              //获取Jedis连接
              val jedis: Jedis = RedisUtil.getJedisClient()
              for (orderWide <- orderWideList) {
                  //3.1从Redis中获取原始金额的累加 orderOriginSum
                  var orderOriginSumKey: String = "order_origin_sum:" + orderWide.order_id
                  var orderOriginSum: Double = 0D
                  val orderOriginSumStr: String = jedis.get(orderOriginSumKey)
                  //注意：从Redis中获取字符串，都要做非空判断
                  if (!Util.isEmpty(orderOriginSumStr)) {
                      orderOriginSum = orderOriginSumStr.toDouble
                  }
                  //3.2从Reids中获取实付分摊金额的累加 orderSplitSum
                  var orderSplitSumKey: String = "order_split_sum:" + orderWide.order_id
                  var orderSplitSum: Double = 0D
                  val orderSplitSumStr: String = jedis.get(orderSplitSumKey)
                  if (!Util.isEmpty(orderSplitSumStr)) {
                      orderSplitSum = orderSplitSumStr.toDouble
                  }
                  //获取每个商品的总额  商品单价*商品数量  后面用于计算比例
                  val detailAmount: Double = orderWide.sku_price * orderWide.sku_num

                  //3.3判断是否为最后一条  计算实付分摊
                  // 如果  原始金额-原始金额的累加==当前商品的总额 那么该商品就是最后一个商品
                  if (orderWide.original_total_amount - orderOriginSum == detailAmount) {
                      orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - orderSplitSum) * 100d) / 100d
                  }
                  //非最后一个商品的情况  最终金额 * 当前商品总额在整个订单中的占比
                  else {
                      val percentage: Double = detailAmount / orderWide.original_total_amount
                      orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * percentage) * 100d) / 100d
                  }

                  //3.4更新Redis中的值
                  //更新原始金额的累加
                  val newOrderOriginSum: Double = orderOriginSum + detailAmount
                  jedis.setex(orderOriginSumKey, 600, newOrderOriginSum.toString)
                  //更新实付分摊金额的累加
                  val newOrderSplitSum: Double = orderSplitSum + orderWide.final_detail_amount
                  jedis.setex(orderSplitSumKey, 600, newOrderSplitSum.toString)

              }
              //关闭连接
              jedis.close()
              orderWideList.toIterator
          })
        OrderWideDStream.print(1000)

        //向ClickHouse中保存数据
        //创建SparkSession对象
        val spark: SparkSession = SparkSession.builder()
          .master("local[3]")
          .appName("sparkSqlOrderWide")
          .getOrCreate()
        import spark.implicits._
        OrderWideDStream.foreachRDD(rdd => {
            //注意:如果程序数据的来源是Kafka，在程序中如果触发多次行动操作(存在分支操作)，DStream或者rdd应该进行缓存
            rdd.cache()
            rdd.toDF.write
              .option("batchsize", "100")
              .option("isolationLevel", "NONE") // 设置事务
              .option("numPartitions", "3") // 设置并发
              .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
              .mode(SaveMode.Append)
              .jdbc("jdbc:clickhouse://hadoop22:8123/default", "t_order_wide", new Properties())


            //将数据写回到Kafka dws_order_wide
            rdd.foreach {
                orderWide => {
                    KafkaUtil.send("dws_order_wide", orderWide)
                }
            }
            OffsetManagerUtil.saveOffset(orderInfoTopic, orderInfoGroupId, OrderInfoApp.offsetRanges)
            OffsetManagerUtil.saveOffset(orderDetailTopic, orderDetailGroupId, OrderDetailApp.offsetRanges)
        })
        SSCUtil.start()

    }
}
