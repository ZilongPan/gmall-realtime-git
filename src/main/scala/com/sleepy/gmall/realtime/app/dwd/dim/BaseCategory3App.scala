package com.sleepy.gmall.realtime.app.dwd.dim

import com.sleepy.gmall.realtime.app.BaseApp
import com.sleepy.gmall.realtime.bean.dwd.dim.BaseCategory3

/**
 * Author: Felix
 * Desc: 读取商品分类维度数据到Hbase
 */
object BaseCategory3App extends BaseApp[BaseCategory3] {
    val topic = "ods_base_category3";
    val groupId = "dim_base_category3_group"
    val tableName = "GMALL_BASE_CATEGORY3"
    val cols = Seq("ID", "NAME", "CATEGORY2_ID")

    def start(): Unit = {
        defaultStart(topic, groupId, tableName, cols);
    }

}

