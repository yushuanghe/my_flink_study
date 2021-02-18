package com.shuanghe.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object DefinedSource {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //自定义source
        val stream1 = env.addSource(new MySensorSource())

        stream1.print()

        env.execute("defined source")
    }
}

//自定义SourceFunction
class MySensorSource() extends SourceFunction[SensorReading] {
    //定义随机数发生器
    val rand: Random.type = Random

    //随机生成一组传感器的初始温度
    var currentTuple: Seq[(String, Double)] = 1.to(10).map(x => ("sensor_" + x, rand.nextDouble() * 100))

    //定义一个flag，用来表示数据源是否正常运行发出数据
    var running: Boolean = true

    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
        //定义无线循环，不停的产生数据，除非被cancel
        while (running) {
            currentTuple = currentTuple.map(data => (data._1, data._2 + rand.nextGaussian()))
            val currentTime = System.currentTimeMillis()
            //ctx.collect 发送数据
            currentTuple.foreach(data => ctx.collect(SensorReading(data._1, currentTime, data._2)))

            Thread.sleep(1000)
        }
    }

    override def cancel(): Unit = {
        running = false
    }
}