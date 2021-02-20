package com.shuanghe.flink.api.source

import org.apache.flink.streaming.api.scala._

//定义样例类，温度传感器
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //1、从集合中读取数据
        val dataList: Seq[SensorReading] = List(SensorReading("s1", 1547718199, 35.8), SensorReading("s6", 1547718201, 15.4), SensorReading("s7", 1547718202, 6.7), SensorReading("s10", 1547718205, 38.1))
        val stream1: DataStream[SensorReading] = env.fromCollection(dataList)
        stream1.print()

        env.fromElements(1.0, 35, "abc").print()
        //2、从文件中读取数据
        val inputPath = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        val stream2: DataStream[String] = env.readTextFile(inputPath)
        stream2.print()
        env.execute("source test")
    }
}
