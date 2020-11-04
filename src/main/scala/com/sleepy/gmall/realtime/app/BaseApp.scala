package com.sleepy.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.sleepy.gmall.realtime.util.{KafkaUtil, OffsetManagerUtil, PhoenixUtil, SSCUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.reflect.{ClassTag, _}

class BaseApp[T <: Product : ClassTag] extends Serializable {

    //TODO 获取当前批次获取偏移量情况
    //var offsetRangeMap: mutable.Map[String, Array[OffsetRange]] = mutable.Map.empty[String, Array[OffsetRange]]
    var offsetRanges: Array[OffsetRange]=Array.empty[OffsetRange]
    def getData(topic: String, groupId: String): DStream[T] = {
        //TODO ==============1.从Kafka中读取数据===============
        //1.1获取偏移量
        val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
        //1.2根据偏移量获取数据
        val recordDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getBaseKafkaStream(topic, SSCUtil.ssc, groupId, offsetMap)

        val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform(rdd => {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        })
        val record: DStream[T] = offsetDStream.map(record => {
            JSON.parseObject(record.value(), myGetClass)
        })

        record
    }

   /* def getData(topics: List[String], groupIds: List[String]): List[DStream[T]] = {
        val list: ListBuffer[DStream[T]] = new ListBuffer[DStream[T]]()
        for (i <- topics.indices) {
            list.append(getData(topics(i), groupIds(i)))
        }
        list.toList
    }*/


    def myGetClass: Class[T] = {
        classTag[T].runtimeClass.asInstanceOf[Class[T]]
    }

    //如果有自己的特殊业务逻辑实现需要自己重写
    def defaultStart(topic: String, groupId: String, tableName: String, cols: Seq[String]): Unit = {
        val objDStream: DStream[T] = getData(topic, groupId)
        objDStream.foreachRDD(rdd => {
            PhoenixUtil.saveRdd(tableName, cols, rdd)
            OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
        })
        SSCUtil.start()
    }


}
