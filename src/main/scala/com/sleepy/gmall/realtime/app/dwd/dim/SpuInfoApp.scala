package com.sleepy.gmall.realtime.app.dwd.dim

import com.sleepy.gmall.realtime.app.BaseApp
import com.sleepy.gmall.realtime.bean.dwd.dim.SpuInfo

/**
 * Author: Felix
 * Desc: 读取商品Spu维度数据到Hbase
 */
object SpuInfoApp extends BaseApp[SpuInfo]{
    val topic = "ods_spu_info";
    val groupId = "dim_spu_info_group"
    val tableName = "GMALL_SPU_INFO"
    val cols = Seq("ID", "SPU_NAME")

    def start(): Unit = {
        defaultStart(topic, groupId, tableName, cols);
    }


}

