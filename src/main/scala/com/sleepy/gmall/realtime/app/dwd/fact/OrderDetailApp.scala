package com.sleepy.gmall.realtime.app.dwd.fact

import com.sleepy.gmall.realtime.app.BaseApp
import com.sleepy.gmall.realtime.bean.dwd.dim.SkuInfo
import com.sleepy.gmall.realtime.bean.dwd.fact.OrderDetail
import com.sleepy.gmall.realtime.util.{KafkaUtil, OffsetManagerUtil, PhoenixUtil, SSCUtil}
import org.apache.spark.streaming.dstream.DStream

/**
 * Desc: 从Kafka的ods_order_detail主题中，读取订单明细数据
 */
object OrderDetailApp extends BaseApp[OrderDetail] {
    val topic = "ods_order_detail";
    val groupId = "order_detail_group"

    def start(): Unit = {
        defaultStart(topic, groupId)
    }

    def defaultStart(topic: String, groupId: String): Unit = {

        val objDStream: DStream[OrderDetail] = getData(topic, groupId)

        //关联维度数据  因为我们这里做了维度退化，所以订单明细直接和商品维度进行关联即可
        //以分区为单位进行处理
        objDStream.mapPartitions(orderDetailItr => {
            val orderDetailList: List[OrderDetail] = orderDetailItr.toList
            //从订单明细中获取所有的商品id
            val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
            //根据商品id到Phoenix中查询出所有的商品
            val sql: String = s"select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name  " +
              s"from gmall_sku_info where id in('${skuIdList.mkString("','")}')"
            val skuInfoMap: Map[String, SkuInfo] = PhoenixUtil.queryList(sql, classOf[SkuInfo])
              .map(skuInfo => {
                  (skuInfo.id, skuInfo)
              }).toMap

            orderDetailList.foreach(orderDetail => {
                val skuInfo: SkuInfo = skuInfoMap.getOrElse(orderDetail.sku_id.toString, null)
                if (skuInfo != null) {
                    orderDetail.spu_id = skuInfo.spu_id.toLong
                    orderDetail.spu_name = skuInfo.spu_name
                    orderDetail.category3_id = skuInfo.category3_id.toLong
                    orderDetail.category3_name = skuInfo.category3_name
                    orderDetail.tm_id = skuInfo.tm_id.toLong
                    orderDetail.tm_name = skuInfo.tm_name
                }
            })
            orderDetailList.toIterator
        })
          //将订单明细数据写回到kafka的DWD层
          .foreachRDD(rdd => {
              rdd.foreach(orderDetail => {
                 KafkaUtil.send("dwd_order_detail",orderDetail)
              })
              //保存偏移量
              OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
          })

        SSCUtil.start()
    }

}
