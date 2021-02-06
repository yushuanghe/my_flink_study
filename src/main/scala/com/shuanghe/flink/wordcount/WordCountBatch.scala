package com.shuanghe.flink.wordcount

import org.apache.flink.api.scala.ExecutionEnvironment
//引入隐式转换
import org.apache.flink.api.scala._

/**
 * 批处理wordcount
 */
object WordCountBatch {
    def main(args: Array[String]): Unit = {
        //
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val inputPath: String = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\zhifubao_20201114.txt"
        val inputDataSet: DataSet[String] = env.readTextFile(inputPath, "utf-8")
        inputDataSet
            .flatMap(line => line.split(""))
            .map(line => (line, 1))
            .groupBy(0)
            .sum(1)
            .print()
    }
}
