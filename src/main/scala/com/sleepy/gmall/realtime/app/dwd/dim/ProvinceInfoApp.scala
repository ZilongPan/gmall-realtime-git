package com.sleepy.gmall.realtime.app.dwd.dim

import com.sleepy.gmall.realtime.app.BaseApp
import com.sleepy.gmall.realtime.bean.dwd.dim.ProvinceInfo

/**
 * Desc:  从Kafka中读取省份数据，保存到Phoenix
 */
object ProvinceInfoApp extends BaseApp[ProvinceInfo] {
    val topic = "ods_base_province"
    val groupId = "province_info_group"

    val tableName = "GMALL_PROVINCE_INFO"
    val cols = Seq("ID", "NAME", "AREA_CODE", "ISO_CODE")

    def start(): Unit = {
        defaultStart(topic, groupId, tableName, cols);
    }
}
