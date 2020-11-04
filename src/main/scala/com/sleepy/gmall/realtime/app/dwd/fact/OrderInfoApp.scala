package com.sleepy.gmall.realtime.app.dwd.fact

import java.time.LocalDateTime

import com.alibaba.fastjson.JSON
import com.sleepy.gmall.realtime.app.BaseApp
import com.sleepy.gmall.realtime.bean.dwd.dim.{ProvinceInfo, UserInfo}
import com.sleepy.gmall.realtime.bean.dwd.fact.{OrderInfo, UserStatus}
import com.sleepy.gmall.realtime.util._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * Desc:  从Kafka中读取订单数据，并对其进行处理
 */
object OrderInfoApp extends BaseApp[OrderInfo] {
    val topic = "ods_order_info"
    val groupId = "order_info_group"
    val tableName = "USER_STATUS"
    val cols = Seq("USER_ID", "IF_CONSUMED")

    def start(): Unit = {
        defaultStart(topic, groupId, tableName, cols)
    }

    override def defaultStart(topic: String, groupId: String, tableName: String, cols: Seq[String]): Unit = {
        val objDStream: DStream[OrderInfo] = getData(topic, groupId)
        //对objDS 设置日期和时间
        objDStream.map(orderInfo => {
            val ldt: LocalDateTime = DateUtil.strToldt(orderInfo.create_time)
            orderInfo.create_date = DateUtil.ldtToDate(ldt)
            orderInfo.create_hour = ldt.getHour.toString
            orderInfo
        })
          //TODO ===================2.判断是否为首单  ====================
          //以分区为单位，进行订单数据处理
          //注意: 由于是按分区处理所以不能在这里进行分组修正(同一个用户本批次内下单多次的情况)
          .mapPartitions(orderInfoItr => {
              //当前一个分区中所有的订单的集合
              val orderInfos: List[OrderInfo] = orderInfoItr.toList
              //获取当前分区中获取下订单的用户id
              val ids: List[Long] = orderInfos.map(_.user_id)
              //根据当前订单数据用户列表, 去phoenix中获取其中下过单的用户id
              val sql: String =
                  s"select USER_ID,IF_CONSUMED from USER_STATUS where user_id in('${ids.mkString("','")}')"
              val idsOrdered: List[String] = PhoenixUtil.queryList(sql)
                .map(_.getString("USER_ID"))
              orderInfos.foreach(orderInfo => {
                  //如果phoenix中存在记录,那么该用户为非首单用户
                  if (idsOrdered.contains(orderInfo.user_id.toString)) {
                      orderInfo.if_first_order = "0"
                  }
                  //如果不存在该用户为首单用户 (这里存在一个用户短时间内下单多次情况,尚未处理)
                  else {
                      orderInfo.if_first_order = "1"
                  }
              })
              orderInfos.toIterator
          })
          //TODO ===================4.同一批次中状态修正  ====================
          //将orderInfos中内容转为  用户id->orderInfo
          .map(orderInfo => {
              (orderInfo.user_id, orderInfo)
          })
          //根据用户id进行分组
          .groupByKey()
          //使用flatMap对每个用户本批次(同一采集周期)订单进行修正处理
          //同一用户的最早的订单标记为首单，其它都改为非首单
          .flatMap {
              case (userId, orderInfoIter) => {
                  val orderInfos: List[OrderInfo] = orderInfoIter.toList
                  if (orderInfos == null || orderInfos.nonEmpty) {
                      orderInfos
                  } else {
                      //如果下了多个订单，按照下单时间升序排序
                      val orderInfosSrot: List[OrderInfo] = orderInfos.sortBy(_.create_time)
                      //只对第一条数据进行判断
                      if (orderInfosSrot.head.if_first_order == "1") {
                          //如果第一条数据是非首单 则将后面的数据 都修正为非首单,第一条数据保持首单不变
                          for (i <- 1 until orderInfosSrot.size) {
                              orderInfosSrot(i).if_first_order = "0"
                          }
                      }
                      orderInfosSrot
                  }
              }
          }
          //TODO ===================5.订单信息和省份维度表进行关联====================
          //以采集周期为单位对数据进行处理 --->通过SQL将所有的省份查询出来, 并通过广播变量进行优化
          .transform(rdd => {
              //从Phoenix中查询所有的省份数据 转换成 (省份id->省份信息)这样的map格式
              val sql: String = "select id,name,area_code,iso_code from gmall_province_info"
              val provinceInfoMap: Map[String, ProvinceInfo] = PhoenixUtil.queryList(sql)
                .map(provinceJsonObj => {
                    //将json对象转换为省份样例类对象
                    val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
                    (provinceInfo.id, provinceInfo)
                }).toMap
              //定义省份的广播变量
              val bdMap: Broadcast[Map[String, ProvinceInfo]] = SSCUtil.ssc.sparkContext.broadcast(provinceInfoMap)

              //遍历订单数据为其设置省份相关信息
              rdd.map(orderInfo => {
                  val proInfo: ProvinceInfo = bdMap.value.getOrElse(orderInfo.province_id.toString, null)
                  if (proInfo != null) {
                      orderInfo.province_name = proInfo.name
                      orderInfo.province_area_code = proInfo.area_code
                      orderInfo.province_iso_code = proInfo.iso_code
                  }
                  orderInfo
              })
          })
          //TODO ===================6.和用户维度表进行关联====================
          //由于用户表数据量会很大, 所以以分区为单位对数据进行处理，每个分区查询一次phoenix
          .mapPartitions(orderInfoItr => {
              //转换为list集合
              val orderInfos: List[OrderInfo] = orderInfoItr.toList
              //获取所有的用户id
              val ids: List[Long] = orderInfos.map(_.user_id)
              //根据id拼接sql语句，到phoenix查询 下过单的用户 相关信息
              //并将用户信息转为 (用户id->用户信息)这样的map
              val sql: String = "select id,user_level,birthday,gender,age_group,gender_name " +
                s"from gmall_user_info where id in ('${ids.mkString("','")}')"
              val usersMap: Map[String, UserInfo] = PhoenixUtil.queryList(sql)
                .map(userJsonObj => {
                    val userInfo: UserInfo = JSON.toJavaObject(userJsonObj, classOf[UserInfo])
                    (userInfo.id, userInfo)
                }).toMap
              //历订单数据为其设置用户相关信息
              orderInfos.foreach(orderInfo => {
                  val userInfo: UserInfo = usersMap.getOrElse(orderInfo.user_id.toString, null)
                  if (userInfo != null) {
                      orderInfo.user_age_group = userInfo.age_group
                      orderInfo.user_gender = userInfo.gender_name
                  }
              })
              orderInfos.toIterator
          })

          //TODO ===================3.维护首单用户状态  ====================
          //如果当前用户为首单用户（第一次消费），我们进行首单标记之后，再将用户信息维护到phoenix中.方便下次查询
          .foreachRDD(rdd => {
              //这里多次使用到了rdd可以先将其做一个缓存
              rdd.cache()
              //3.1 维护首单用户状态
              //将首单用户过滤出来
              val userStatusRDD: RDD[UserStatus] = rdd.filter(_.if_first_order == "1")
                //将rdd中的orderInfo数据转化为UserStatus 方便进行保存
                //注意：在使用saveToPhoenix方法的时候，要求RDD中存放数据的属性个数和Phoenix表中字段数必须要一致
                .map(orderInfo => UserStatus(orderInfo.user_id.toString, "1"))
              PhoenixUtil.saveRdd(tableName, cols, userStatusRDD)

              //3.2保存订单数据到ES中 并且写回到kafka-dwd层
              rdd.foreachPartition(orderInfoItr => {
                  val orderInfos: List[OrderInfo] = orderInfoItr.toList
                  if (!Util.isEmpty(orderInfos)) {
                      //转换和格式写到ES
                      val orderInfosMap: List[(String, OrderInfo)] = orderInfos
                        .map(orderInfo => (orderInfo.id.toString, orderInfo))
                      ESUtil.bulkInsert(orderInfosMap, "gmall_order_info_" + orderInfos.head.create_date)
                      //3.4写回到Kafka
                      orderInfos.foreach(orderInfo => {
                          KafkaUtil.send("dwd_order_info", orderInfo)
                      })
                  }
              })
              OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
          })

        SSCUtil.start()
    }

}
