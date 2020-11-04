package com.sleepy.gmall.realtime.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * 读取配置文件的工具类
 */
object PropertiesUtil {
    //读取配置文件
    val properties: Properties = PropertiesUtil.load("config.properties")

    //PropertiesUtil测试
   /* def main(args: Array[String]): Unit = {
        val prop: Properties = PropertiesUtil.load("config.properties")
        println(prop.getProperty("kafka.broker.list"))
    }*/

    def load(propertiesName: String): Properties = {
        val prop: Properties = new Properties()
        //加载指定的配置文件
        prop.load(new InputStreamReader(
            //通过线程类加载器获取配置文件
            //Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
            //通过系统类加载器获取配置文件
            ClassLoader.getSystemClassLoader.getResourceAsStream(propertiesName),
            StandardCharsets.UTF_8))
        prop
    }

    def zookeeperAddress: String = {
        properties.getProperty("zookeeper.node.address")
    }
}
