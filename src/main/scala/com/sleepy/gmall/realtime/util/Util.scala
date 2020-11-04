package com.sleepy.gmall.realtime.util

object Util {
    def isEmpty(datas: Iterable[Any]): Boolean = {
        datas == null || datas.isEmpty
    }

    def isEmpty(str: String): Boolean = {
        str == null || str.isEmpty
    }
}
