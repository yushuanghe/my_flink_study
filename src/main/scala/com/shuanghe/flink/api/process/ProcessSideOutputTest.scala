package com.shuanghe.flink.api.process

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import java.util.Properties

object ProcessSideOutputTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val prop = new Properties()
        prop.setProperty("bootstrap.servers", "localhost:9092")
        prop.setProperty("group.id", "consumer-group")

        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        prop.setProperty("auto.offset.reset", "latest")

        val dataStream: DataStream[SensorReading] = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), prop))
            .map(data => {
                val arr = data.split(",")
                SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
            })

        val splitTemp = 30.0
        val majorStream = dataStream
            .process(new SplitTempProcessor(splitTemp))

        majorStream.print("major")

        majorStream.getSideOutput(new OutputTag[(String, Long, Double)]("minor"))
            .print("minor")

        env.execute("process side output test")
    }
}

/**
 * 自定义 ProcessFunction ，实现分流
 *
 * @param splitTemp
 */
class SplitTempProcessor(splitTemp: Double) extends ProcessFunction[SensorReading, SensorReading] {
    override def onTimer(timestamp: Long, ctx: ProcessFunction[SensorReading, SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = super.onTimer(timestamp, ctx, out)

    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
        if (value.temperature > splitTemp) {
            //输出到主流
            out.collect(value)
        } else {
            //输出到侧输出流
            ctx.output(new OutputTag[(String, Long, Double)]("minor"), (value.id, value.timestamp, value.temperature))
        }
    }
}
