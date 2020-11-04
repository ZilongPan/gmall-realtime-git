package com.sleepy.gmall.realtime.util

import java.io.File
import java.util

import com.sleepy.gmall.realtime.common.Constant
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
 *  维护偏移量的工具类
 */
object OffsetManagerUtil {


    //从Redis中获取偏移量
    // key: offset:topic:groupId
    //      hashkey:partition   value: 偏移量
    val OFFSET_PREFIX = "offset"


    def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
        val jedis: Jedis = RedisUtil.getJedisClient()
        //拼接操作redis的key     offset:topic:groupId
        val offsetKey: String = OFFSET_PREFIX + Constant.REDIS_SEPARATOR + topic + Constant.REDIS_SEPARATOR + groupId

        //获取当前消费者组消费的主题  对应的分区以及偏移量
        val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
        jedis.close()

        //将java的map转换为scala的map
        import scala.collection.JavaConverters._
        val oMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
            case (partition, offset) => {
                //println("读取分区偏移量：" + partition + ":" + offset)
                //Map[TopicPartition,Long]
                (new TopicPartition(topic, partition.toInt), offset.toLong)
            }
        }.toMap
        oMap
    }


    //将偏移量信息保存到Redis中
    def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
        //拼接redis中操作偏移量的key
        val offsetKey: String = OFFSET_PREFIX + Constant.REDIS_SEPARATOR + topic + Constant.REDIS_SEPARATOR + groupId
        //定义java的map集合，用于存放每个分区对应的偏移量
        val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
        //对offsetRanges进行遍历，将数据封装offsetMap
        offsetRanges.foreach(offsetRange=>{
            //分区id
            val partitionId: Int = offsetRange.partition
            //开始位置
            val fromOffset: Long = offsetRange.fromOffset
            //结束位置
            val untilOffset: Long = offsetRange.untilOffset
            offsetMap.put(partitionId.toString, untilOffset.toString)
            //println("保存分区" + partitionId + ":" + fromOffset + "----->" + untilOffset)
        })
        val jedis: Jedis = RedisUtil.getJedisClient()
        println("offsetMap:  "+offsetMap)
        if(!offsetMap.isEmpty){
            jedis.hmset(offsetKey, offsetMap)
        }
        jedis.close()
    }
}
