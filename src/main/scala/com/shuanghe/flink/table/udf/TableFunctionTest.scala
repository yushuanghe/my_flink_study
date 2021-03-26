package com.shuanghe.flink.table.udf

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionTest {
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
        val splitUdtf = new SplitUdtf("_")
        val resultTable = sensorTable
            .joinLateral(splitUdtf('id) as('word, 'word_length))
            .select('id, 'ts, 'word, 'word_length)
        resultTable.printSchema()
        resultTable.toAppendStream[Row].print()

        //sql
        tableEnv.registerFunction("split_udtf", splitUdtf)
        tableEnv.createTemporaryView("sensor", sensorTable)
        val resultSqlTable = tableEnv.sqlQuery(
            """
              |select
              |id,ts,word,word_length
              |from sensor
              |,lateral table (split_udtf(id)) as tmp (word,word_length)
              |""".stripMargin)
        resultSqlTable.printSchema()
        resultSqlTable.toAppendStream[Row].print("sql")

        env.execute("udtf test")
    }
}

/**
 * 自定义表函数
 * udtf
 */
class SplitUdtf(separator: String) extends TableFunction[(String, Int)] {
    def eval(input: String): Unit = {
        input.split(separator).foreach(word => collect((word, word.length)))
    }
}