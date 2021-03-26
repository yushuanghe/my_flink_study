package com.shuanghe.flink.table.udf

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggregateFunctionTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
            .useOldPlanner()
            .inStreamingMode()
            .build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

        val inputPath: String = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        val inputStream: DataStream[String] = env.readTextFile(inputPath, "utf-8")
        val dataStream: DataStream[SensorReading] = inputStream
            .map(data => {
                val arr = data.split("\t")
                SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
                override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
            })

        val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

        //table api
        val tempAvgUdaf = new TempAvgUdaf
        val resultTable = sensorTable
            .groupBy('id)
            .aggregate(tempAvgUdaf('temperature) as 'temp_avg)
            .select('id, 'temp_avg)
        resultTable.printSchema()
        resultTable.toRetractStream[Row].print()

        //sql
        tableEnv.createTemporaryView("sensor", sensorTable)
        tableEnv.registerFunction("temp_avg_udaf", tempAvgUdaf)
        val resultSqlTable = tableEnv.sqlQuery(
            """
              |select
              |id,temp_avg_udaf(temperature) as temp_avg2
              |from sensor
              |group by id
              |""".stripMargin)
        resultSqlTable.printSchema()
        resultSqlTable.toRetractStream[Row].print("sql")

        env.execute("udaf test")
    }
}

/**
 * 定义聚合状态 accumulator
 */
class TempAvgAcc() {
    var sum: Double = 0
    var cnt: Int = 0
}

/**
 * 自定义聚合函数
 * udaf
 */
class TempAvgUdaf extends AggregateFunction[Double, TempAvgAcc] {
    override def getValue(accumulator: TempAvgAcc): Double = accumulator.sum / accumulator.cnt

    override def createAccumulator(): TempAvgAcc = new TempAvgAcc

    /**
     * 实现具体的处理计算函数
     * 相当于eval
     */
    def accumulate(accumulator: TempAvgAcc, input: Double): Unit = {
        accumulator.sum += input
        accumulator.cnt += 1
    }
}
