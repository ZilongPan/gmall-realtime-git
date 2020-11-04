package com.sleepy.gmall.realtime

import scala.beans.BeanProperty

case class LogStart2(
                     @BeanProperty var dt: String,
                     @BeanProperty var common: LogStart2#Common2,
                     @BeanProperty var start: LogStart2#Start2,
                     @BeanProperty var hr: String,
                     @BeanProperty var ts: Long
                   ) {

    case class Common2(
                       @BeanProperty var ar: String,
                       @BeanProperty var uid: String,
                       @BeanProperty var os: String,
                       @BeanProperty var ch: String,
                       @BeanProperty var md: String,
                       @BeanProperty var mid: String,
                       @BeanProperty var vc: String,
                       @BeanProperty var ba: String
                     )

    case class Start2(
                      @BeanProperty var entry: String,
                      @BeanProperty var open_ad_skip_ms: Int,
                      @BeanProperty var open_ad_ms: Int,
                      @BeanProperty var loading_time: Int,
                      @BeanProperty var open_ad_id: Int
                    )

}
