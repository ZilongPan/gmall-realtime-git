package com.sleepy.gmall.realtime.app.ods

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.sleepy.gmall.realtime.bean.ods.MaxwellKafka
import com.sleepy.gmall.realtime.util.{KafkaUtil, OffsetManagerUtil, SSCUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 从Kafka中读取数据，根据表名进行分流处理（maxwell）
 */
object BaseDBMaxwellApp {
    val topic = "gmall_db_m"
    val groupId = "base_db_maxwell_group"

    def main(args: Array[String]): Unit = {


        //从Redis中获取偏移量
        val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
        val recordDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getBaseKafkaStream(topic, SSCUtil.ssc, groupId, offsetMap)

        //获取当前采集周期中读取的主题对应的分区以及偏移量
        var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
        val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }
        println("------可以启动生产者了------")
        //对读取的数据进行结构的转换   ConsumerRecord<K,V> ==>V(jsonStr)==>V(jsonObj)
        val maxwellKafkaDStream: DStream[MaxwellKafka] = offsetDStream.map {
            record => {
                JSON.parseObject(record.value(), classOf[MaxwellKafka])
            }
        }
        //分流
        maxwellKafkaDStream.foreachRDD(rdd => {
            rdd.foreach(maxwellKafka => {
                //获取操作的数据
                val dataJsonObj: JSONObject = maxwellKafka.data
                //判断操作数据不为空  且  操作类型为insert
                if (dataJsonObj != null && !dataJsonObj.isEmpty) {
                    //如果数据不为空则对数据进行进一步判断
                    //获取表名
                    val tableName: String = maxwellKafka.table
                    //获取操作类型
                    val opType: String = maxwellKafka.`type`
                    val allowTables = List("base_province", "user_info", "sku_info", "base_trademark", "base_category3", "spu_info")
                    if (
                        ("order_info".equals(tableName) && "insert".equals(opType))
                          || ("order_detail".equals(tableName) && "insert".equals(opType))
                          || allowTables.contains(tableName)
                    ) {
                        //拼接要发送到的主题 "ods_表名"
                        val sendTopic: String = "ods_" + tableName
                        KafkaUtil.send(sendTopic, JSON.toJSONString(dataJsonObj, SerializerFeature.PrettyFormat))
                    }
                }
            })
            //手动提交偏移量
            OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
        })

        SSCUtil.start()
    }
}
