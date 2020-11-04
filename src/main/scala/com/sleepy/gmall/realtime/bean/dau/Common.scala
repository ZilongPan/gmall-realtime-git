package com.sleepy.gmall.realtime.bean.dau

import scala.beans.BeanProperty

case class Common(
                       @BeanProperty var ar: String,//地区
                       @BeanProperty var uid: String,//用户id
                       @BeanProperty var os: String,
                       @BeanProperty var ch: String,//渠道
                       @BeanProperty var md: String,
                       @BeanProperty var mid: String,//设备id
                       @BeanProperty var vc: String,//版本
                       @BeanProperty var ba: String
                     )
