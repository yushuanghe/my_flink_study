package com.shuanghe.flink.api.transform

import org.apache.flink.streaming.api.scala._
import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, IterationRuntimeContext, MapFunction, ReduceFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration

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
            .filter(new MyFilter("s1"))

        val aggStream = dataStream
            //分组聚合，输出每个传感器当前最小值
            .keyBy("id")
            .minBy("temperature")
        //            .min("temperature")

        //reduce操作
        val resultStream = dataStream
            .keyBy("id")
            //            .reduce((currentData: SensorReading, newData: SensorReading) => {
            //                SensorReading(currentData.id, currentData.timestamp.max(newData.timestamp), currentData.temperature.min(newData.temperature))
            //            })
            .reduce(new MyReduceFunction)

        //resultStream.print()

        //多流转换操作
        //分流 split
        val splitStream: SplitStream[SensorReading] = dataStream
            .split(data => {
                if (data.temperature > 30.0) Seq("high") else Seq("low")
            })
        val highStream: DataStream[SensorReading] = splitStream.select("high")
        val lowStream = splitStream.select("low")
        val allStream = splitStream.select("high", "low")

        //highStream.print("high")
        //lowStream.print("low")
        //allStream.print("all")

        //合流操作 connect
        val warningStream: DataStream[(String, Double)] = highStream.map(data => (data.id, data.temperature))
        val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowStream)

        val coMapResultStream: DataStream[Product] = connectedStream
            .map(warningData => (warningData._1, warningData._2, "warning"), lowData => (lowData.id, "healthy"))

        coMapResultStream.print("coMap")

        //union合流
        val unionStream = highStream.union(lowStream)

        env.execute("transform test")
    }
}

class MyReduceFunction extends ReduceFunction[SensorReading] {
    override def reduce(currentData: SensorReading, newData: SensorReading): SensorReading = {
        SensorReading(currentData.id, currentData.timestamp.max(newData.timestamp), currentData.temperature.min(newData.temperature))
    }
}

//自定义函数类
class MyFilter(keyword: String) extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = {
        value.id.startsWith(keyword)
    }
}

class MyMapper extends MapFunction[SensorReading, String] {
    override def map(value: SensorReading): String = {
        value.id + " temperature"
    }
}

//富函数，可以获取到运行时上下文，还有一些生命周期方法
class MyRichMapper extends RichMapFunction[SensorReading, String] {
    override def map(value: SensorReading): String = {
        value.id + " temperature"
    }

    override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

    //生命周期方法
    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //做一些初始化的操作，比如数据库连接
    }

    //生命周期方法
    override def close(): Unit = {
        super.close()
        //一般做收尾工作，比如关闭连接，或者清空状态
    }
}
