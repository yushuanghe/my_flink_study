package com.shuanghe.flink.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FileOutputTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val settingsOld: EnvironmentSettings = EnvironmentSettings.newInstance()
            .useOldPlanner()
            .inStreamingMode()
            .build()
        val oldStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settingsOld)

        val inputPath: String = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        val tableSchema: Schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("temperature", DataTypes.DOUBLE())
        oldStreamTableEnv.connect(new FileSystem().path(inputPath))
            .withFormat(new Csv().fieldDelimiter('\t'))
            .withSchema(tableSchema)
            .createTemporaryTable("input_table")

        val sensorTable: Table = oldStreamTableEnv.from("input_table")
            .select('id, 'temperature)
        //.filter('id === "s1")

        val aggTable: Table = sensorTable
            .groupBy('id)
            .select('id, 'id.count as 'cnt)

        //输出到文件
        oldStreamTableEnv.connect(new FileSystem().path
        ("C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\output\\sensor_output.txt"))
            .withFormat(new Csv().fieldDelimiter('\073'))
            .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                //.field("cnt", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE())
            )
            .createTemporaryTable("file_output")

        aggTable.toRetractStream[(String, Long)].print()

        //AppendStreamTableSink requires that Table has only insert changes.
        //aggTable.insertInto("file_output")

        sensorTable.insertInto("file_output")

        env.execute("file output")
    }
}
