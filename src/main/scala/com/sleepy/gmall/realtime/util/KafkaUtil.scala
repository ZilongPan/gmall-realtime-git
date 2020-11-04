package com.sleepy.gmall.realtime.util

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}

/**
 * 读取kafka的工具类
 */
object KafkaUtil {
    val broker_list = PropertiesUtil.properties.getProperty("kafka.broker.list")

    //定义默认消费者组为dau_group
    val GROUP_ID = "gmall_group"
    //初始化生产者
    var kafkaProducer: KafkaProducer[String, String] = createKafkaProducer

    // kafka消费者配置
    var kafkaParam = collection.mutable.Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
        //消费者组配置
        ConsumerConfig.GROUP_ID_CONFIG -> GROUP_ID,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        //latest自动重置偏移量为最新的偏移量 默认值"latest"
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
        //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
        //如果是false，会需要手动维护kafka偏移量
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    def getBaseKafkaStream(topic: String, ssc: StreamingContext, groupId: String = GROUP_ID, offsets: Map[TopicPartition, Long] = null)
    : InputDStream[ConsumerRecord[String, String]] = {
        //如果传递了 groupId 那么就是用传入的groupId
        kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId

        //如果偏移量为空则从最新数据开始消费
        // 如果偏移量不为空那么就使用偏移量
        var consumerStrategy: ConsumerStrategy[String, String] = null
        if (Util.isEmpty(offsets)) {
            consumerStrategy = ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
        } else {
            consumerStrategy = ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets)
        }

        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            consumerStrategy
        )
        dStream
    }


    //kafka生产者配置
    def createKafkaProducer: KafkaProducer[String, String] = {
        val properties = new Properties
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list)
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, (true: java.lang.Boolean))
        new KafkaProducer[String, String](properties)
    }

    def send(topic: String, msg: String): Unit = {
        kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
    }

    def send(topic: String, key: String, msg: String): Unit = {
        kafkaProducer.send(new ProducerRecord[String, String](topic, key, msg))
    }

    def send(topic: String, obj: AnyRef): Unit = {
        val msg: String = JSON.toJSONString(obj, new SerializeConfig(true), SerializerFeature.PrettyFormat)
        KafkaUtil.send(topic, msg)
    }

    //即将废弃代码
    /*// 创建DStream，返回接收到的输入数据   使用配置中默认的消费者组
    def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
        )
        dStream
    }

    //在对Kafka数据进行消费的时候，指定消费者组
    def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
        kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
        dStream
    }

    //从指定的偏移量位置读取数据
    def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String, offsets: Map[TopicPartition, Long])
    : InputDStream[ConsumerRecord[String, String]] = {
        kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets))
        dStream
    }*/
}
