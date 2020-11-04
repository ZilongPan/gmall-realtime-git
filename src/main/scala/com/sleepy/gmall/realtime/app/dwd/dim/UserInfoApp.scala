package com.sleepy.gmall.realtime.app.dwd.dim

import java.time.{Duration, LocalDateTime}

import com.sleepy.gmall.realtime.app.BaseApp
import com.sleepy.gmall.realtime.bean.dwd.dim.UserInfo
import com.sleepy.gmall.realtime.util.{DateUtil, PhoenixUtil, SSCUtil}
import org.apache.spark.streaming.dstream.DStream

/**
 * Desc: 从Kafka中读取数据，保存到Phoenix
 */
object UserInfoApp extends BaseApp[UserInfo] {
    val topic = "ods_user_info"
    val groupId = "user_info_group"
    val tableName = "GMALL_USER_INFO"
    val cols = Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME")

    def start(): Unit = {
        defaultStart(topic, groupId, tableName, cols);
    }

    override def defaultStart(topic: String, groupId: String, tableName: String, cols: Seq[String]): Unit = {
        //1.2获取偏移量
        val objDstream: DStream[UserInfo] = getData(topic, groupId)
        //1.4对从Kafka中读取的数据进行结构的转换  record(kv)==>UserInfo
        objDstream.map(userInfo => {
            //把生日转成年龄
            /*val formattor = new SimpleDateFormat("yyyy-MM-dd")
            val date: Date = formattor.parse(userInfo.birthday)
            val curTs: Long = System.currentTimeMillis()
            val betweenMs: Long = curTs - date.getTime
            val age: Long = betweenMs / 1000L / 60L / 60L / 24L / 365L*/
            val birthday: LocalDateTime = DateUtil.strToldt(userInfo.birthday)
            val age: Long = Duration.between(birthday, LocalDateTime.now).getSeconds / 60L / 60L / 24L / 365L
            if (age < 20) {
                userInfo.age_group = "20岁及以下"
            } else if (age > 30) {
                userInfo.age_group = "30岁以上"
            } else {
                userInfo.age_group = "21岁到30岁"
            }
            if (userInfo.gender == "M") {
                userInfo.gender_name = "男"
            } else {
                userInfo.gender_name = "女"
            }
            userInfo
        })
          //1.5保存到Phoenix中
          .foreachRDD(rdd => {
              PhoenixUtil.saveRdd(tableName, cols, rdd)
          })
        SSCUtil.start()
    }
}
