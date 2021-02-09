package com.shuanghe.flink.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//引入隐式转换
import org.apache.flink.streaming.api.scala._

/**
 * 流式处理wordcount
 */
object WordCountStream {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(16)

        val paramTool: ParameterTool = ParameterTool.fromArgs(args)
        val host: String = paramTool.get("host")
        val port: Int = paramTool.getInt("port")

        val inputDataStream: DataStream[String] = env.socketTextStream(host, port)

        inputDataStream
            .flatMap(line => line.split(""))
            .filter(_.nonEmpty)
            .map(line => (line, 1)).setParallelism(3)
            .keyBy(0)
            .sum(1).setParallelism(2)
            .print().setParallelism(1)

        env.execute("word_count_stream")
    }
}
