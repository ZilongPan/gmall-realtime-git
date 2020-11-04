package com.sleepy.gmall.realtime.app.dwd.dim

import com.sleepy.gmall.realtime.app.BaseApp
import com.sleepy.gmall.realtime.bean.dwd.dim.BaseTrademark

/**
 * Author: Felix
 * Date: 2020/10/30
 * Desc: 从Kafka中读取品牌维度数据，保存到Hbase
 */
object BaseTrademarkApp extends BaseApp[BaseTrademark] {
    val topic = "ods_base_trademark";
    val groupId = "dim_base_trademark_group"

    val tableName = "GMALL_BASE_TRADEMARK"
    val cols = Seq("ID", "TM_NAME")

    def start(): Unit = {
        defaultStart(topic, groupId, tableName, cols);
    }


}
