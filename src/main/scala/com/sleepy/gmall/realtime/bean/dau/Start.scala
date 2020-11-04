package com.sleepy.gmall.realtime.bean.dau

import scala.beans.BeanProperty

case class Start(
                  @BeanProperty var entry: String,
                  @BeanProperty var open_ad_skip_ms: Int,
                  @BeanProperty var open_ad_ms: Int,
                  @BeanProperty var loading_time: Int,
                  @BeanProperty var open_ad_id: Int
                )
