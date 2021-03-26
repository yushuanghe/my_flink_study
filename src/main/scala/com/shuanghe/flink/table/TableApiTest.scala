package com.shuanghe.flink.table

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}

import java.sql.Timestamp

object TableApiTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val inputPath: String = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        val inputStream: DataStream[String] = env.readTextFile(inputPath, "utf-8")

        val dataStream: DataStream[SensorReading] = inputStream
            .map(data => {
                val arr = data.split("\t")
                SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
            })

        //创建表执行环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

        //基于流创建一张表
        val dataTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp, 'temperature, 'pt.proctime)

        //调用table api进行转换
        val resultTable: Table = dataTable
            .select("id,temperature,pt")
            .filter("id='s1'")

        resultTable.printSchema()

        resultTable.toAppendStream[(String, Double, Timestamp)]
            .print("result")

        //直接用sql实现
        tableEnv.createTemporaryView("dataTable", dataTable)
        val sql = "select id,temperature from dataTable where id='s1'"
        val resultSqlTable: Table = tableEnv.sqlQuery(sql)

        resultSqlTable.printSchema()

        resultSqlTable.toAppendStream[(String, Double)]
            .print("result_sql")

        env.execute("table example")
    }
}
