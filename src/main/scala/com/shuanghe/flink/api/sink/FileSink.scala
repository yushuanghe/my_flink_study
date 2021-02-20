package com.shuanghe.flink.api.sink

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object FileSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputPath = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        val inputStream: DataStream[String] = env.readTextFile(inputPath)

        val dataStream = inputStream
            .map(data => {
                val arr = data.split("\t")
                SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
            })

        dataStream.print()
        dataStream.writeAsCsv("C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\output\\out.txt", null, "\n", "\t")
        dataStream.addSink(StreamingFileSink.forRowFormat(
            new Path("C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\output\\out1.txt")
            , new SimpleStringEncoder[SensorReading]()).build()
        )

        env.execute("kafka sink")
    }
}
