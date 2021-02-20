package com.shuanghe.flink.api.window

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
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

        val resultStream = dataStream
            .keyBy(data => data.id)
            //滚动时间窗口
            //.window(TumblingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3)))
            //滑动时间窗口
            //.window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(5), Time.seconds(3)))
            //会话窗口
            //.window(EventTimeSessionWindows.withGap(Time.seconds(30)))
            //简写
            //.timeWindow(Time.seconds(15), Time.seconds(5))
            //计数window
            .countWindow(50)
            .minBy("temperature")
            .print()

        env.execute("es sink")
    }
}
