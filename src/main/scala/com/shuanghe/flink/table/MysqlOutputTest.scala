package com.shuanghe.flink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object MysqlOutputTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val settingsOld: EnvironmentSettings = EnvironmentSettings.newInstance()
            .useOldPlanner()
            .inStreamingMode()
            .build()
        val oldStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settingsOld)

        val tableSchema: Schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("temperature", DataTypes.DOUBLE())

        val inputPath: String = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        oldStreamTableEnv.connect(new FileSystem().path(inputPath))
            .withFormat(new Csv().fieldDelimiter('\t'))
            .withSchema(tableSchema)
            .createTemporaryTable("csv_input_table")

        val sensorTable = oldStreamTableEnv.from("csv_input_table")
        val resultTable = sensorTable
            .select('id, 'temperature)
        //.filter('id === "s1")

        val aggTable = resultTable
            .groupBy('id)
            .select('id, 'id.count as 'cnt)

        val sinkDDL: String =
            """
              |create table jdbc_output_table (
              |id varchar(32) not null,
              |cnt bigint not null
              |) with (
              |'connector.type'='jdbc',
              |'connector.url'='jdbc:mysql://localhost:3306/test',
              |'connector.table'='sensor_count',
              |'connector.driver'='com.mysql.jdbc.Driver',
              |'connector.username'='root',
              |'connector.password'='123456'
              |)
              |""".stripMargin

        //执行ddl
        oldStreamTableEnv.sqlUpdate(sinkDDL)
        aggTable.insertInto("jdbc_output_table")

        env.execute("mysql output test")
    }
}
