package com.sleepy.gmall.realtime.app.dwd.dim

import com.sleepy.gmall.realtime.app.BaseApp
import com.sleepy.gmall.realtime.bean.dwd.dim.{BaseCategory3, BaseTrademark, SkuInfo, SpuInfo}
import com.sleepy.gmall.realtime.util._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * Author: Felix
 * Desc: 读取商品维度数据，并关联品牌、分类、Spu，保存到Hbase
 */
object SkuInfoApp extends BaseApp[SkuInfo] {
    val topic = "ods_sku_info";
    val groupId = "dim_sku_info_group"
    val tableName = "GMALL_SKU_INFO"
    val cols = Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME")

    def start(): Unit = {
        defaultStart(topic, groupId, tableName, cols);
    }


    override def defaultStart(topic: String, groupId: String, tableName: String, cols: Seq[String]): Unit = {
        val objDstream: DStream[SkuInfo] = getData(topic, groupId)
        //商品和品牌、分类、Spu先进行关联
        val skuInfoDstream: DStream[SkuInfo] = objDstream.transform(rdd => {
            //tm_name
            val tmSql = "select id ,tm_name  from gmall_base_trademark"
            val tmMap: Map[String, BaseTrademark] = PhoenixUtil.queryList(tmSql, classOf[BaseTrademark])
              .map(item => {
                  (item.id, item)
              }).toMap
            /*val tmList: List[JSONObject] = PhoenixUtil.queryList(tmSql)
            val tmMap: Map[String, JSONObject] = tmList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap*/

            //category3
            val category3Sql = "select id ,name from gmall_base_category3" //driver  周期性执行
            val category3Map: Map[String, BaseCategory3] = PhoenixUtil.queryList(category3Sql, classOf[BaseCategory3])
              .map(item => {
                  (item.id, item)
              }).toMap
            /*val category3List: List[JSONObject] = PhoenixUtil.queryList(category3Sql)
            val category3Map: Map[String, JSONObject] = category3List.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap*/

            // spu
            val spuSql = "select id ,spu_name  from gmall_spu_info" // spu
            val spuMap: Map[String, SpuInfo] = PhoenixUtil.queryList(spuSql, classOf[SpuInfo])
              .map(item => {
                  (item.id, item)
              }).toMap
            /*val spuList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
            val spuMap: Map[String, JSONObject] = spuList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap*/

            // 汇总到一个list 广播这个map
            val dimBC: Broadcast[(Map[String, BaseCategory3], Map[String, BaseTrademark], Map[String, SpuInfo])] =
                SSCUtil.ssc.sparkContext.broadcast((category3Map, tmMap, spuMap))

            val skuInfoRDD: RDD[SkuInfo] = rdd.map {
                skuInfo => {
                    //接收bc
                    val category3Map: Map[String, BaseCategory3] = dimBC.value._1
                    val tmMap: Map[String, BaseTrademark] = dimBC.value._2
                    val spuMap: Map[String, SpuInfo] = dimBC.value._3

                    val category3Obj: BaseCategory3 = category3Map.getOrElse(skuInfo.category3_id, null) //从map中寻值
                    if (category3Obj != null) {
                        skuInfo.category3_name = category3Obj.name
                    }

                    val tmObj: BaseTrademark = tmMap.getOrElse(skuInfo.tm_id, null) //从map中寻值
                    if (tmObj != null) {
                        skuInfo.tm_name = tmObj.tm_name
                    }
                    val spuObj: SpuInfo = spuMap.getOrElse(skuInfo.spu_id, null) //从map中寻值
                    if (spuObj != null) {
                        skuInfo.spu_name = spuObj.spu_name
                    }
                    skuInfo
                }
            }
            skuInfoRDD

        })
        //保存到Hbase
        skuInfoDstream.foreachRDD(rdd => {
            PhoenixUtil.saveRdd(tableName, cols, rdd)
        })


        SSCUtil.start()
    }


}

