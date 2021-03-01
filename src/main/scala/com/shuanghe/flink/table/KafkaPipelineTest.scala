package com.shuanghe.flink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object KafkaPipelineTest {
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
        oldStreamTableEnv.connect(new Kafka()
            .version("universal")
            .topic("sensor")
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")
        )
            .withFormat(new Csv().fieldDelimiter(','))
            .withSchema(tableSchema)
            .createTemporaryTable("kafka_input_table")

        val sensorTable = oldStreamTableEnv.from("kafka_input_table")
        val resultTable = sensorTable
            .select('id, 'temperature)
            .filter('id === "s1")

        val aggTable = resultTable
            .groupBy('id)
            .select('id, 'id.count as 'cnt)

        val outTableSchema: Schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("temperature", DataTypes.DOUBLE())
        oldStreamTableEnv.connect(new Kafka()
            .version("universal")
            .topic("kafka_sink")
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")
        )
            .withFormat(new Csv().fieldDelimiter(','))
            .withSchema(outTableSchema)
            .createTemporaryTable("kafka_output_table")

        resultTable.insertInto("kafka_output_table")

        env.execute("kafka pipeline test")
    }
}
