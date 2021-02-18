package com.shuanghe.flink.transform

import org.apache.flink.streaming.api.scala._
import com.shuanghe.flink.source.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction

object TransformTest {
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

        val aggStream = dataStream
            //分组聚合，输出每个传感器当前最小值
            .keyBy("id")
            .minBy("temperature")
        //            .min("temperature")

        val resultStream = dataStream
            .keyBy("id")
            //            .reduce((currentData: SensorReading, newData: SensorReading) => {
            //                SensorReading(currentData.id, currentData.timestamp.max(newData.timestamp), currentData.temperature.min(newData.temperature))
            //            })
            .reduce(new MyReduceFunction)

        resultStream.print()

        env.execute("transform test")
    }
}

class MyReduceFunction extends ReduceFunction[SensorReading] {
    override def reduce(currentData: SensorReading, newData: SensorReading): SensorReading = {
        SensorReading(currentData.id, currentData.timestamp.max(newData.timestamp), currentData.temperature.min(newData.temperature))
    }
}