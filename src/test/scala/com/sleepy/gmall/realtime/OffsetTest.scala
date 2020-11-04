package com.sleepy.gmall.realtime

import com.sleepy.gmall.realtime.app.dwd.fact.{OrderDetailApp, OrderInfoApp}

object OffsetTest {
    def main(args: Array[String]): Unit = {

        println( OrderDetailApp.offsetRanges eq OrderInfoApp.offsetRanges)
    }
}
