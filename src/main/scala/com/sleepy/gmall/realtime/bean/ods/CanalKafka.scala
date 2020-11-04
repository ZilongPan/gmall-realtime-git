package com.sleepy.gmall.realtime.bean.ods

import com.alibaba.fastjson.JSONObject

import scala.beans.BeanProperty

case class CanalKafka(
                       //数据
                       @BeanProperty var data: Array[JSONObject],
                       //数据库名
                       @BeanProperty var database: String,
                       @BeanProperty var es: Long,
                       @BeanProperty var id: Long,
                       @BeanProperty var isDdl: Boolean,
                       @BeanProperty var mysqlType: JSONObject,
                       @BeanProperty var old: Array[JSONObject],
                       @BeanProperty var pkNames: Array[String],
                       @BeanProperty var sql: String,
                       @BeanProperty var sqlType: JSONObject,
                       //表名
                       @BeanProperty var table: String,
                       @BeanProperty var ts: Long,
                       //操作数据库的类型 INSERT
                       @BeanProperty var `type`: String
                     )
