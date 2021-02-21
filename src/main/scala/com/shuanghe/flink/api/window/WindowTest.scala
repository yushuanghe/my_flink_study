package com.shuanghe.flink.api.window

import com.shuanghe.flink.api.source.{MySensorSource, SensorReading}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputPath = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        val inputStream: DataStream[String] = env.readTextFile(inputPath)

        //val dataStream: DataStream[SensorReading] = inputStream
        //    .map(data => {
        //        val arr = data.split("\t")
        //        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        //    })

        val dataStream: DataStream[SensorReading] = env.addSource(new MySensorSource)

        //dataStream.print()

        val resultStream = dataStream
            .keyBy(data => data.id)

            /**
             * Rather than that,if you are living in somewhere which is not using UTC±00:00 time
             * , such as China which is using UTC+08:00,and you want a time window with size of one day
             * , and window begins at every 00:00:00 of local time,you may use of(Time.days(1),Time.hours(-8)).
             * The parameter of offset is Time.hours(-8)) since UTC+08:00 is 8 hours earlier than UTC time.
             */
            //滚动时间窗口
            //.window(TumblingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3)))
            //滑动时间窗口
            //.window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(5), Time.seconds(3)))
            //会话窗口
            //.window(EventTimeSessionWindows.withGap(Time.seconds(30)))
            //简写
            //.timeWindow(Time.seconds(15), Time.seconds(5))
            .timeWindow(Time.seconds(15))
            //计数window
            //.countWindow(3)
            //[trigger]
            //[evictor]
            //[allowedLateness]
            //[sideOutputLateData]
            //.minBy("temperature")
            .reduce((data1, data2) => SensorReading(data1.id, data1.timestamp.max(data2.timestamp)
                , data1.temperature.min(data2.temperature)))
        //[getSideOutput]

        resultStream.print()

        env.execute("window test")
    }
}

class MyReducer extends ReduceFunction[SensorReading] {
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
        SensorReading(value1.id, value1.timestamp.max(value2.timestamp)
            , value1.temperature.min(value2.temperature))
    }
}