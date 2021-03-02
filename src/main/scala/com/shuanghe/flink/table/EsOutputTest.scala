package com.shuanghe.flink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Kafka, Schema}

object EsOutputTest {
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

        oldStreamTableEnv.connect(new Elasticsearch()
            .version("7")
            .host("localhost", 9200, "http")
            .index("sensor_table")
            .documentType("temperature"))
            .inUpsertMode()
            .withFormat(new Json)
            .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("cnt", DataTypes.BIGINT()))
            .createTemporaryTable("es_output_table")

        aggTable.insertInto("es_output_table")

        val sinkDD="""
                |
                |""".stripMargin

        env.execute("es output test")
    }
}
