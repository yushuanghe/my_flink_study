package com.shuanghe.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaSource {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val prop = new Properties()
        prop.setProperty("bootstrap.servers", "localhost:9092")
        prop.setProperty("group.id", "consumer-group")

        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        prop.setProperty("auto.offset.reset", "latest")

        val stream = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), prop))
    }
}
