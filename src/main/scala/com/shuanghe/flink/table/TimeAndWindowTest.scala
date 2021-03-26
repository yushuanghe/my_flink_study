package com.shuanghe.flink.table

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}
import org.apache.flink.types.Row

import java.sql.Timestamp
import java.util.Date

object TimeAndWindowTest {
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

        //可以改变流字段顺序
        //有的source未实现 DefinedProctimeAttribute 接口，无法定义 proctime，如 CsvTableSource
        //val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)

        //val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp)
        val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)
        //sensorTable.printSchema()
        ////sensorTable.toAppendStream[Row].print()
        //sensorTable.toAppendStream[(String, Double, Timestamp)].print()

        ////有的source未实现 DefinedProctimeAttribute 接口，无法定义 proctime，如 CsvTableSource
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //tableEnv.connect(new FileSystem().path(inputPath))
        //    .withFormat(new Csv().fieldDelimiter('\t'))
        //    .withSchema(new Schema()
        //        .field("id", DataTypes.STRING())
        //        .field("timestamp", DataTypes.BIGINT()).rowtime(new Rowtime()
        //        //某字段设为 eventtime
        //        .timestampsFromField("timestamp")
        //        //watermark 延迟1秒
        //        .watermarksPeriodicBounded(1000)
        //    )
        //        .field("temperature", DataTypes.DOUBLE())
        //    )
        //    .createTemporaryTable("csv_input_table")
        //val sensorTableEventTime = tableEnv.from("csv_input_table")
        //sensorTableEventTime.printSchema()
        //sensorTableEventTime.toAppendStream[Row].print()

        //1、group window
        //1.1、table api
        val resultTable = sensorTable
            //每10秒统计一次
            //滚动窗口
            .window(Tumble.over(10.seconds).on('ts).as('gw))
            .groupBy('gw, 'id)
            //'gw.end 当前窗口的结束时间
            .select('id, 'id.count, 'temperature.avg, 'ts.min, 'gw.end)
        resultTable.printSchema()
        //resultTable.toRetractStream[Row].print("result")

        //1.2、sql
        tableEnv.createTemporaryView("sensor", sensorTable)
        //tumble(ts,interval '10' second) sql中窗口定义
        val resultSqlTable = tableEnv.sqlQuery(
            """
              |select
              |id,count(id) as ic_cnt,avg(temperature) as temp_avg,min(ts) as tx_min
              |,tumble_end(ts,interval '10' second) as gw_end
              |from sensor
              |group by id,tumble(ts,interval '10' second)
              |""".stripMargin)
        resultSqlTable.printSchema()
        resultSqlTable.toAppendStream[(String, Long, Double, Timestamp, Timestamp)].print("result_sql")
        resultSqlTable.toRetractStream[Row].print("result_sql")

        //2、over window
        //2.1、table api
        //统计每个sensor每条数据，与之前两行数据的平均温度
        val overResultTable = sensorTable
            .window(Over.partitionBy('id).orderBy('ts).preceding(2.rows).as('ow))
            .select('id, 'ts, 'id.count.over('ow) as 'a, 'temperature.avg.over('ow) as 'b)
        overResultTable.printSchema()
        overResultTable.toAppendStream[Row].print("over")

        //2.2、sql
        val overResultSqlTable = tableEnv.sqlQuery(
            """
              |select
              |id,ts
              |,count(id) over ow as id_cnt
              |,avg(temperature) over ow as temp_avg
              |from sensor
              |window ow as (partition by id order by ts rows between 2 preceding and current row)
              |""".stripMargin)
        overResultSqlTable.printSchema()
        overResultSqlTable.toAppendStream[Row].print("over_sql")

        env.execute("time and window test")
    }
}
