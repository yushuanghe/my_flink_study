package com.shuanghe.flink.sink

import com.shuanghe.flink.source.SensorReading
import org.apache.flink.streaming.api.scala._

object EsSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputPath = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        val inputStream: DataStream[String] = env.readTextFile(inputPath)

        val dataStream: DataStream[SensorReading] = inputStream
            .map(data => {
                val arr = data.split("\t")
                SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
            })

        //dataStream.addSink(new ElasticsearchSink[SensorReading]())

        env.execute("es sink")
    }
}
