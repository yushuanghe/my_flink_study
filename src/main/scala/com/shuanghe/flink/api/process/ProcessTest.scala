package com.shuanghe.flink.api.process

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import java.util.Properties

object ProcessTest {
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
        //.keyBy(data => data.id)
        //.process(new MyKeyedProcessFunction)
        val interval = 10 * 1000L
        val warningStream = dataStream
            .keyBy(data => data.id)
            .process(new TempIncreaseWarning(interval))

        warningStream.print()

        env.execute("process test")
    }
}

/**
 * process 定时器
 *
 * threshold 内，温度连续上升，发出数据
 * 保存上一次状态，不上升时则删除计时器，计时器不删除时代表一直上升，发送数据
 *
 * @param interval
 */
class TempIncreaseWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {
    /**
     * 定义状态，保存上一个温度进行比较，保存注册定时器的时间戳用于删除定时器
     */
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]
    ("lastTempState", classOf[Double]))
    lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]
    ("timerTsState", classOf[Long]))

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
        super.onTimer(timestamp, ctx, out)
        out.collect("传感器" + ctx.getCurrentKey + "温度连续" + interval / 1000 + "秒上升")
        timerTsState.clear()
    }

    override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def close(): Unit = super.close()

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
        //获取状态
        val lastTemp = lastTempState.value()
        out.collect("lastTempState的值" + lastTemp)

        lastTempState.update(value.temperature)

        val timerTs = timerTsState.value()
        out.collect("timerTs的值" + timerTs)

        val ts = ctx.timerService().currentProcessingTime() + interval
        //比较温度
        if (value.temperature > lastTemp) {
            //此时才注册一个定时器
            ctx.timerService().registerProcessingTimeTimer(ts)
            timerTsState.update(ts)
        } else if (value.temperature <= lastTemp) {
            //温度不上升，删除定时器
            ctx.timerService().deleteProcessingTimeTimer(timerTs)
            timerTsState.clear()
            out.collect("clear的值" + timerTsState.value())
        }
    }
}

/**
 * KeyedProcessFunction 样例
 */
class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)

    override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def close(): Unit = super.close()

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
        ctx.getCurrentKey
        ctx.timestamp()
        ctx.timerService().currentWatermark()
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000L)
        ctx.timerService().deleteEventTimeTimer(ctx.timestamp() + 60000L)
    }
}
