package com.shuanghe.flink.api.window

import com.shuanghe.flink.api.source.{MySensorSource, SensorReading}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * window操作与watermark
 */
object WindowTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //时间语义设置
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //watermark更新间隔
        env.getConfig.setAutoWatermarkInterval(500)

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

            //val dataStream: DataStream[SensorReading] = env.addSource(new MySensorSource)

            //提取时间戳 无乱序数据流使用
            //.assignAscendingTimestamps(data => data.timestamp * 1000L)
            //最大乱序程度 Time.seconds(3)
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading]
            (Time.seconds(3)) {
                override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
            })

        val lateTag = new OutputTag[SensorReading]("late")

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
            //窗口在watermark达到右区间+1分钟后关闭
            .allowedLateness(Time.minutes(1))
            //[sideOutputLateData]
            .sideOutputLateData(lateTag)
            //.minBy("temperature")
            .reduce((data1, data2) => SensorReading(data1.id, data1.timestamp.max(data2.timestamp)
                , data1.temperature.min(data2.temperature)))
        //[getSideOutput]

        resultStream.getSideOutput(lateTag).print("late")

        resultStream.print("result")

        env.execute("window test")
    }
}

class MyReducer extends ReduceFunction[SensorReading] {
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
        SensorReading(value1.id, value1.timestamp.max(value2.timestamp)
            , value1.temperature.min(value2.temperature))
    }
}

//周期性watermark
class MyPeriodicWatermark extends AssignerWithPeriodicWatermarks[SensorReading] {
    //延迟 60s
    val bound: Long = 60 * 1000L
    var maxTs: Long = Long.MinValue

    /**
     * 周期性生成
     * env.getConfig.setAutoWatermarkInterval(500)
     *
     * @return
     */
    override def getCurrentWatermark: Watermark = {
        new Watermark(maxTs - bound)
    }

    /**
     * 来一条调用一次
     *
     * @param element
     * @param previousElementTimestamp
     * @return
     */
    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
        maxTs = maxTs.max(element.timestamp * 1000L)
        element.timestamp * 1000L
    }
}

//间断式watermark
class MyPunctuatedWatermark extends AssignerWithPunctuatedWatermarks[SensorReading] {
    //延迟 60s
    val bound: Long = 60 * 1000L

    /**
     * 事件触发，每来一条判断是否生产watermark
     *
     * @param lastElement
     * @param extractedTimestamp
     * @return
     */
    override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
        if (lastElement.id == "sensor_1") {
            new Watermark(extractedTimestamp - bound)
        } else {
            null
        }
    }

    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
        element.timestamp * 1000L
    }
}
