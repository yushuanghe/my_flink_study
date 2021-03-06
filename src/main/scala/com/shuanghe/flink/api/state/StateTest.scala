package com.shuanghe.flink.api.state

import com.shuanghe.flink.api.source.SensorReading
import com.shuanghe.flink.api.transform.MyReduceFunction
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import java.{lang, util}
import java.util.Properties
import java.util.concurrent.TimeUnit

object StateTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.setStateBackend(new MemoryStateBackend())
        env.setStateBackend(new FsStateBackend("hdfs://", true))
        env.setStateBackend(new RocksDBStateBackend(""))

        //checkpoint
        //时间间隔是JobManager给source任务触发checkpoint的时间间隔
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setCheckpointTimeout(60000L)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
        //会覆盖 setMaxConcurrentCheckpoints
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
        env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

        //重启策略
        //固定时间间隔重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10,
            TimeUnit.SECONDS)))

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

        //温度跳变，超过10度，报警
        val threshold = 10.0
        val alertStream: DataStream[(String, Double, Double)] = dataStream
            .keyBy(data => data.id)
            //.flatMap(new TempChangeAlert(10.0))
            //有状态的flatmap
            //必须先定义泛型
            .flatMapWithState[(String, Double, Double), Double]({
                case (data: SensorReading, None) => (List.empty, Some(data.temperature)) //初识第一条，不输出
                case (data: SensorReading, lastTemp: Some[Double]) => {
                    val newTemp = data.temperature
                    val diff = (newTemp - lastTemp.get).abs
                    if (diff > threshold) {
                        (List((data.id, lastTemp.get, newTemp)), Some(newTemp))
                    } else {
                        //不输出
                        (List.empty, Some(newTemp))
                    }
                }
            })

        alertStream.print()

        env.execute("state test")
    }
}

/**
 * 自定义 RichFlatMapFunction
 *
 * @param threshold 温度跳变报警阈值
 */
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]
    ("last_temp", classOf[Double]))

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
    }

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
        val lastTemp: Double = lastTempState.value()
        val newTemp = value.temperature
        lastTempState.update(newTemp)

        val diff = (newTemp - lastTemp).abs
        if (diff > threshold) {
            out.collect((value.id, lastTemp, newTemp))
        }
    }
}

/**
 * keyed state测试，必须定义在 RichFunction中，因为需要 getRuntimeContext
 */
class MyRichMapFunc extends RichMapFunction[SensorReading, String] {
    var valueState: ValueState[Double] = _

    lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("list_state",
        classOf[Int]))
    lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]
    ("map_state", classOf[String], classOf[Double]))

    lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new
            ReducingStateDescriptor[SensorReading]("reducing_state", new MyReduceFunction, classOf[SensorReading]))

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("value_state",
            classOf[Double]))
    }

    override def map(value: SensorReading): String = {
        //获取状态
        val tmpState: Double = valueState.value()
        //更新状态
        valueState.update(value.temperature)

        listState.add(value.timestamp.toInt)
        val tmpList = new util.ArrayList[Int]()
        tmpList.add(1)
        tmpList.add(2)
        listState.addAll(tmpList)
        listState.update(tmpList)
        val t: lang.Iterable[Int] = listState.get()

        mapState.contains("s1")
        val v1: Double = mapState.get("s1")
        mapState.put(value.id, value.temperature)

        val r: SensorReading = reduceState.get()
        //调用 ReduceFunction 做聚合
        reduceState.add(value)

        value.id
    }
}