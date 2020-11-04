package com.sleepy.gmall.realtime.service

import java.lang
import java.util

import com.sleepy.gmall.realtime.bean.dau.{DauInfo, LogStart}
import com.sleepy.gmall.realtime.util.{ESUtil, RedisUtil}
import io.searchbox.client.JestClient
import io.searchbox.core.{Bulk, Index}
import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

import scala.collection.Iterator

object DauService extends Serializable {


    def insertDauSingle(dauKey: String, mid: String): Long = {
        val jedisClient: Jedis = RedisUtil.getJedisClient()
        val isNew: Long = jedisClient.sadd(dauKey, mid)
        //设置当天的key数据失效时间为24小时 (只有首次创建添加该key的时候才设置)
        if (jedisClient.ttl(dauKey) == -1) {
            jedisClient.expire(dauKey, 3600 * 24)
        }
        jedisClient.close()
        isNew
    }

    def insertDauBath(logStarts: List[LogStart]): List[LogStart] = {
        //每一个分区获取一次Redis的连接
        val jedis: Jedis = RedisUtil.getJedisClient()
        val filteredList: List[LogStart] = logStarts.filter(logStart => {
            //获取设备mid
            val mid: String = logStart.common.mid
            //拼接向Redis放的数据的key dau:日期
            val dauKey: String = "dau:" + logStart.getDt
            //判断Redis中是否存在该数据
            val isFirst: Long = jedis.sadd(dauKey, mid)
            //设置当天的key数据失效时间为24小时 (只有首次创建添加该key的时候才设置)
            if (jedis.ttl(dauKey) < 0L) {
                jedis.expire(dauKey, 3600 * 24)
            }
            //过滤得到 redis中不存在的数据
            if (isFirst == 1L) {
                true
            } else {
                false
            }
        })
        jedis.close()
        filteredList
    }

}
