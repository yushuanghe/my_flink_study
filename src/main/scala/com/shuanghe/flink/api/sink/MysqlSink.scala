package com.shuanghe.flink.api.sink

import com.shuanghe.flink.api.source.{MySensorSource, SensorReading}
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

object MysqlSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputPath = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        val inputStream: DataStream[String] = env.readTextFile(inputPath)

        //val dataStream: DataStream[SensorReading] = inputStream
        //    .map(data => {
        //        val arr = data.split("\t")
        //        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        //    })

        val dataStream: DataStream[SensorReading] = env.addSource(new MySensorSource())

        dataStream.addSink(new MyMysqlSinkFunc)

        env.execute("mysql sink")
    }
}

class MyMysqlSinkFunc extends RichSinkFunction[SensorReading] {
    //定义连接
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/docker_test", "root", "123456")
        insertStmt = conn.prepareStatement("insert into docker_test.sensor_temp (id,temp) values (?,?)")
        updateStmt = conn.prepareStatement("update docker_test.sensor_temp set temp=? where id=?")
    }

    override def close(): Unit = {
        super.close()
        insertStmt.close()
        updateStmt.close()
        conn.close()
    }

    override def invoke(value: SensorReading): Unit = {
        super.invoke(value)
        updateStmt.setDouble(1, value.temperature)
        updateStmt.setString(2, value.id)
        updateStmt.execute()

        if (updateStmt.getUpdateCount == 0) {
            insertStmt.setString(1, value.id)
            insertStmt.setDouble(2, value.temperature)
            insertStmt.execute()
        }
    }

    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = super.invoke(value, context)
}
