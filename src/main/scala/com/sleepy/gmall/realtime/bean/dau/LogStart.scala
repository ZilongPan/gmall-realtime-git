package com.sleepy.gmall.realtime.bean.dau

import java.time.LocalDate

import scala.beans.BeanProperty

case class LogStart(
                     @BeanProperty var dt: LocalDate,
                     @BeanProperty var common: Common,
                     @BeanProperty var start: Start,
                     @BeanProperty var hr: Int,
                     @BeanProperty var ts: Long
                   )
