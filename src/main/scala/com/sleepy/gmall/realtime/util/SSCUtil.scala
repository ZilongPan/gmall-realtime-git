package com.sleepy.gmall.realtime.util

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SSCUtil {
    //解决docker内无用户的报错
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("sleepy"))
    //local[*] 设置的数量最好和kafka分区数一致
    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("App")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //启动过程
    def start(): Unit = {
        ssc.start()
        ssc.awaitTermination()
    }
}
