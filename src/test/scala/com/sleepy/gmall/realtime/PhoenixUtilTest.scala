package com.sleepy.gmall.realtime

import com.alibaba.fastjson.JSONObject
import com.sleepy.gmall.realtime.bean.dwd.dim.ProvinceInfo
import com.sleepy.gmall.realtime.util.PhoenixUtil

object PhoenixUtilTest {
    def main(args: Array[String]): Unit = {
        val jsonObj: List[JSONObject] = PhoenixUtil.queryList("select * from gmall_province_info")
        println(jsonObj)
        val infoes: List[ProvinceInfo] = PhoenixUtil.queryList("select * from gmall_province_info", classOf[ProvinceInfo])
        println(infoes)

    }


}
