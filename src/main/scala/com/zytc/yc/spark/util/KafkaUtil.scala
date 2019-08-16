package com.zytc.yc.spark.util

import com.typesafe.config.Config
import com.zytc.yc.spark.common.GlobalConstant
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaUtil {
    def Kafka(ssc: StreamingContext, i: Int) = {
        val broker = GlobalConstant.KAFKA_BROKER
        val topic = GlobalConstant.KAFKA_TOPIC
        //kafka的参数配置
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> broker,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "auto.offset.reset" -> "latest",
            "group.id" -> ("g" + i),
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val topics = Array(topic)
        val streamData = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
        streamData
    }
}
