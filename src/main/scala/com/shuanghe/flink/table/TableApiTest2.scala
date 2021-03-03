package com.shuanghe.flink.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

object TableApiTest2 {
    def main(args: Array[String]): Unit = {
        //1、创建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

        //1.1、基于老版本planner的流处理
        val settingsOld: EnvironmentSettings = EnvironmentSettings.newInstance()
            .useOldPlanner()
            .inStreamingMode()
            .build()
        val oldStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settingsOld)

        //1.2、基于老版本的批处理
        val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

        //1.3、基于blink planner的流处理
        val blinkStreamSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()
        val blinkStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamSettings)

        //1.4、基于blink planner的批处理
        val blinkBatchSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inBatchMode()
            .build()
        val blinkBatchTableEnv: TableEnvironment = TableEnvironment.create(blinkBatchSettings)

        //2、连接外部系统，读取数据，注册表
        val tableSchema: Schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("temperature", DataTypes.DOUBLE())

        //2.1、读取文件
        val inputPath: String = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        oldStreamTableEnv.connect(new FileSystem().path(inputPath))
            .withFormat(new Csv().fieldDelimiter('\t'))
            .withSchema(tableSchema)
            .createTemporaryTable("csv_input_table")

        //2.2、从kafka读取数据
        oldStreamTableEnv.connect(new Kafka()
            .version("universal")
            .topic("sensor")
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")
        )
            .withFormat(new Csv().fieldDelimiter(','))
            .withSchema(tableSchema)
            .createTemporaryTable("kafka_input_table")

        //3、查询转换
        //3.1、使用table api
        val resultTable: Table = oldStreamTableEnv.from("csv_input_table")
            //Expression表达式
            .select('id, 'temperature)
            .filter('id === "s1")

        //3.2、sql
        val resultSqlTable: Table = oldStreamTableEnv.sqlQuery(
            """
              |select id,temperature
              |from csv_input_table
              |where id='s1'
              |""".stripMargin)

        val inputTable: Table = oldStreamTableEnv.from("kafka_input_table")
        inputTable.toAppendStream[(String, Long, Double)].print()
        resultTable.toAppendStream[(String, Double)].print("table_api")
        resultSqlTable.toAppendStream[(String, Double)].print("sql")

        val explain = oldStreamTableEnv.explain(resultSqlTable)
        println(explain)

        env.execute("table api test")
    }
}
