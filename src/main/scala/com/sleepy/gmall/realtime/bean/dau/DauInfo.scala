package com.sleepy.gmall.realtime.bean.dau

import scala.beans.BeanProperty

case class DauInfo(
                    @BeanProperty mid: String, //设备id
                    @BeanProperty uid: String, //用户id
                    @BeanProperty ar: String, //地区
                    @BeanProperty ch: String, //渠道
                    @BeanProperty vc: String, //版本
                    @BeanProperty var dt: String, //日期
                    @BeanProperty var hr: String, //小时
                    @BeanProperty var mi: String, //分钟
                    @BeanProperty var ts: Long //时间戳
                  )
