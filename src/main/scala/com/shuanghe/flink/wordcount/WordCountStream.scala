package com.shuanghe.flink.wordcount

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//引入隐式转换
import org.apache.flink.streaming.api.scala._

/**
 * 批处理wordcount
 */
object WordCountStream {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 6666)

        inputDataStream
            .flatMap(line => line.split(""))
            .filter(_.nonEmpty)
            .map(line => (line, 1))
            .keyBy(0)
            .sum(1)
            .print()

        env.execute("word_count_stream")
    }
}
