package com.sleepy.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * 获取Jedis客户端的工具类
  */
object RedisUtil {
  //定义一个连接池对象
  private var jedisPool:JedisPool = _
  //初始化jedisPool
  build()

  //创建JedisPool连接池对象
  def build():Unit = {
    val host: String = PropertiesUtil.properties.getProperty("redis.host")
    val port: String = PropertiesUtil.properties.getProperty("redis.port")

    val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
    jedisPoolConfig.setMaxTotal(100)  //最大连接数
    jedisPoolConfig.setMaxIdle(20)   //最大空闲
    jedisPoolConfig.setMinIdle(20)     //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

    jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
  }
  //获取Jedis客户端
  def getJedisClient():Jedis ={
    jedisPool.getResource
  }
  //测试方法
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = getJedisClient()
    jedis.sadd("dau:2020-10-17","mid22")
    println(jedis.ping())
    jedis.close()

  }
}
