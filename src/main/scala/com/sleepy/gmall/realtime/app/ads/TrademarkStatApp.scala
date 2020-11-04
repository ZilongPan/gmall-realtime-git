package com.sleepy.gmall.realtime.app.ads

import com.alibaba.fastjson.JSON
import com.sleepy.gmall.realtime.bean.dws.OrderWide
import com.sleepy.gmall.realtime.util.{DateUtil, KafkaUtil, OffsetManagerM, SSCUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

import scala.collection.mutable.ListBuffer

object TrademarkStatApp {
    val groupId = "ads_trademark_stat_group"
    val topic = "dws_order_wide";

    def start(): Unit = {
        defaultStart()
    }


    def defaultStart(): Unit = {
        //从Mysql中读取偏移量
        val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerM.getOffset(topic, groupId)

        //把偏移量传递给kafka ，加载数据流
        val recordInputDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getBaseKafkaStream(topic, SSCUtil.ssc, groupId, offsetMapForKafka)

        //从流中获得本批次的 偏移量结束点
        var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
        val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform(rdd => {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        })

        //提取数据
        val tradermarkSumDstream: DStream[(String, Double)] = inputGetOffsetDstream.map(record => {
            //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
            val orderWide: OrderWide = JSON.parseObject(record.value(), classOf[OrderWide])
            orderWide
        })
          // 聚合
          .map(orderWide => {
              (orderWide.tm_id + "_" + orderWide.tm_name, orderWide.final_detail_amount)
          }).reduceByKey(_ + _)



        //存储数据以及偏移量到MySQL中，为了保证精准消费   我们将使用事务对存储数据和修改偏移量进行控制
        //方式2：批量插入
        tradermarkSumDstream.foreachRDD(rdd => {
            // 为了避免分布式事务，把ex的数据提取到driver中;因为做了聚合，所以可以直接将Executor的数据聚合到Driver端
            val tmSumArr: Array[(String, Double)] = rdd.collect()
            if (tmSumArr != null && tmSumArr.size > 0) {
                DBs.setup()
                DB.localTx {
                    implicit session => {
                        // 写入计算结果数据
                        //获取写入数据的时间
                        val dateTime: String = DateUtil.nowDateTimeStr
                        val batchParamsList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()
                        for ((tm, amount) <- tmSumArr) {
                            val amountRound: Double = Math.round(amount * 100D) / 100D
                            val tmArr: Array[String] = tm.split("_")
                            val tmId: String = tmArr(0)
                            val tmName: String = tmArr(1)
                            batchParamsList.append(Seq(dateTime, tmId, tmName, amountRound))
                        }
                        //val params: Seq[Seq[Any]] = Seq(Seq("2020-08-01 10:10:10","101","品牌1",2000.00),
                        // Seq("2020-08-01 10:10:10","102","品牌2",3000.00))
                        //数据集合作为多个可变参数 的方法 的参数的时候 要加:_*
                        SQL("insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)")
                          .batch(batchParamsList: _*).apply()
                        //throw new RuntimeException("测试异常")

                        // 写入偏移量
                        for (offsetRange <- offsetRanges) {
                            val partitionId: Int = offsetRange.partition
                            val untilOffset: Long = offsetRange.untilOffset
                            SQL("replace into offset  values(?,?,?,?)").bind(groupId, topic, partitionId, untilOffset).update().apply()
                        }

                    }
                }
            }
        })


        SSCUtil.start()
    }

}