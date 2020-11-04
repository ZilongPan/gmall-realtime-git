package com.sleepy.gmall.realtime.app.dau

import java.time.LocalDateTime

import com.alibaba.fastjson.JSON
import com.sleepy.gmall.realtime.bean.dau.{DauInfo, LogStart}
import com.sleepy.gmall.realtime.service.DauService
import com.sleepy.gmall.realtime.util.{DateUtil, ESUtil, KafkaUtil, OffsetManagerUtil, SSCUtil, Util}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * Desc:  日活业务
 */
object DauApp {
    //定义Dau消费主题 为启动日志
    val TOPIC = "gmall_start"
    //定义Dau消费者组为dau_group
    val GROUP_ID = "dau_group"

    def main(args: Array[String]): Unit = {
        //TODO 从redis中获取偏移量
        //如果Redis中存在当前消费者组对该主题的偏移量信息，那么从执行的偏移量位置开始消费
        //如果Redis中没有当前消费者组对该主题的偏移量信息，那么还是按照配置，从最新位置开始消费
        val offset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(TOPIC, GROUP_ID)
        //TODO 通过SparkStreaming程序从Kafka中读取数据
        val recordDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getBaseKafkaStream(TOPIC, SSCUtil.ssc, GROUP_ID, offset)


        //获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值
        var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
        val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
            rdd => {
                //因为recodeDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }

        println("------可以启动生产者了------")


        //转换json数据格式为LogStart
        val logStartDStream: DStream[LogStart] = offsetDStream.map {
            item => {
                //在数据中 增加 dt(日期精确到天)和 hr(小时) 两个字段
                val logStart: LogStart = JSON.parseObject(item.value(), classOf[LogStart])
                val ldt: LocalDateTime = DateUtil.tsToldt(logStart.ts)
                logStart.setDt(ldt.toLocalDate)
                logStart.setHr(ldt.getHour)
                logStart
            }
        }
        // 临时打印测试
        // logStartDStream.print(1000)


        // TODO 获取当前新活跃的用户filteredDStream 并将当日已经登录过的用户存入redis
        // 方案1:filter
        // 缺点：虽然我们从池中获取Reids，但是每次从流中取数据都进行过滤，连接还是过于频繁
        /*val filteredDStream: DStream[LogStart] = logStartDStream.filter {
            logStart => {
                //获取设备mid
                val mid: String = logStart.common.mid
                //拼接向Redis放的数据的key dau:日期
                val dauKey: String = "dau:" + logStart.getDt
                val isNew: Long = DauService.insertDauSingle(dauKey, mid)
                //Reids不存在，我们需要从DS流中将数据过滤出来，同时数据会保存到Redis中
                if (isNew == 1L) {
                    true
                }
                //Reids中已经存在该，我们需要把该数据从DS流中过滤掉
                else {
                    false
                }
            }
        }*/

        // 方案2:mapPartions
        //以分区为单位进行过滤，可以减少和连接池交互的次数
        val filteredDStream: DStream[LogStart] = logStartDStream.mapPartitions {
            //以分区为单位对数据进行处理
            logStarts => {
                DauService.insertDauBath(logStarts.toList).toIterator
            }
        }
        // 输出测试 方案1,2
        // 数量会越来越少，最后变为0   因为我们mid只是模拟了50个
        //filteredDStream.count().print()

        //TODO 将过滤后的数据放于ES中
        filteredDStream.foreachRDD(rdd => {
            //以分区为单位对数据进行处理
            rdd.foreachPartition(logStartIter => {
                val logStarts: List[LogStart] = logStartIter.toList
                val dauInfos: List[(String, DauInfo)] = logStarts.map(item => {
                    val dauInfo: DauInfo = DauInfo(
                        item.common.mid,
                        item.common.uid,
                        item.common.ar,
                        item.common.ch,
                        item.common.vc,
                        DateUtil.ldToDate(item.dt),
                        item.hr.toString,
                        "00",
                        item.ts
                    )
                    (item.common.mid, dauInfo)
                })

                //将数据批量的保存到ES中
                ESUtil.bulkInsert(dauInfos, "gmall_dau_info_" + logStarts.head.dt)
            })
            //提交偏移量到Redis中
            OffsetManagerUtil.saveOffset(TOPIC, GROUP_ID, offsetRanges)
        })


        //启动saprkStreaming
        SSCUtil.start()
    }
}
