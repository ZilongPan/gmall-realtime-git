package com.sleepy.gmall.realtime.bean.ods

import com.alibaba.fastjson.JSONObject

import scala.beans.BeanProperty

case class MaxwellKafka(
                         @BeanProperty var database: String,
                         @BeanProperty var table: String,
                         @BeanProperty var `type`: String,
                         @BeanProperty var ts: Long,
                         @BeanProperty var xid: Long,
                         @BeanProperty var xoffset: Long,
                         @BeanProperty var data: JSONObject
                       )
