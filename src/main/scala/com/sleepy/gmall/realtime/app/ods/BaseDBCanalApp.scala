package com.sleepy.gmall.realtime.app.ods

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.sleepy.gmall.realtime.bean.ods.CanalKafka
import com.sleepy.gmall.realtime.util.{KafkaUtil, OffsetManagerUtil, SSCUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}


/**
  *  从Kafka中读取数据，根据表名进行分流处理（canal）
  */
object BaseDBCanalApp {
  val topic = "gmall_db_c"
  val groupId = "base_db_canal_group"

  def main(args: Array[String]): Unit = {

    //从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    val recordDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getBaseKafkaStream(topic,SSCUtil.ssc,groupId,offsetMap)

    //获取当前批次读取的Kafka主题中偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    println("------可以启动生产者了------")
    //对接收到的数据进行结构的转换，ConsumerRecord[String,String(jsonStr)]====>jsonObj
    val canalKafkaDStream: DStream[CanalKafka] = offsetDStream.map {
      record => {
         JSON.parseObject(record.value(), classOf[CanalKafka])
      }
    }

    //分流：根据不同的表名，将数据发送到不同的kafka主题中去
    canalKafkaDStream.foreachRDD{
      rdd=>{
        rdd.foreach{
          canalKafka=>{
            //判断操作类型是否为INSERT
            if("INSERT".equals(canalKafka.`type`)){
              //获取操作数据
              val dataArr: Array[JSONObject] = canalKafka.data
              //拼接目标topic名称 "ods_表名"
              val sendTopic: String = "ods_" + canalKafka.table
              //对data数组进行遍历  根据表名将数据发送到不同的主题中去
              dataArr.foreach(item=>{
                val data: String = JSON.toJSONString(item,SerializerFeature.PrettyFormat)
                KafkaUtil.send(sendTopic,data)
              })
            }
          }
        }
        //提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    SSCUtil.start()

  }
}
