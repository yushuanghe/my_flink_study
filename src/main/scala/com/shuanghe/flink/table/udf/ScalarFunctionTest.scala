package com.shuanghe.flink.table.udf

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionTest {
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
        val hashCodeUdf = new HashCodeUdf(666)
        val daliMapUdf = new DaliMapUdf()

        val resultTable = sensorTable
            .select('id, 'ts, hashCodeUdf('id), daliMapUdf('id))
        resultTable.printSchema()
        resultTable.toAppendStream[Row].print()

        //sql
        tableEnv.createTemporaryView("sensor", sensorTable)
        tableEnv.registerFunction("hash_code_udf", hashCodeUdf)
        tableEnv.registerFunction("dali_udf", daliMapUdf)

        val resultSqlTable = tableEnv.sqlQuery(
            """
              |select id,ts,hash_code_udf(id),dali_udf(id) from sensor
              |""".stripMargin)
        resultSqlTable.printSchema()
        resultSqlTable.toAppendStream[Row].print("sql")

        env.execute("scalar udf")
    }
}

/**
 * 自定义标量函数
 * udf
 */
class HashCodeUdf(factor: Int) extends ScalarFunction {
    def eval(input: String): Int = {
        input.hashCode * factor - 1000
    }
}

class DaliMapUdf extends ScalarFunction {
    def eval(input: String): String = input + "大力出奇迹！"
}
