package com.shuanghe.flink.table.udf

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregateFunctionTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
            .useOldPlanner()
            .inStreamingMode()
            .build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

        val inputPath: String = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        val inputStream: DataStream[String] = env.readTextFile(inputPath, "utf-8")
        val dataStream: DataStream[SensorReading] = inputStream
            .map(data => {
                val arr = data.split("\t")
                SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
                override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
            })

        val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

        //table api
        val tempTop2Udtaf = new TempTop2Udtaf
        val resultTable = sensorTable
            .groupBy('id)
            .flatAggregate(tempTop2Udtaf('temperature) as('temp, 'rank))
            .select('id, 'temp, 'rank)
        resultTable.printSchema()
        resultTable.toRetractStream[Row].print()

        //sql
        //sql 不好实现

        env.execute("table aggregate function test")
    }
}

/**
 * 定义聚合状态 accumulator
 */
class TempTop2Acc {
    var highestTemp: Double = Double.MinValue
    var secondHighestTemp: Double = Double.MinValue
}

/**
 * 自定义表聚合函数
 */
class TempTop2Udtaf extends TableAggregateFunction[(Double, Int), TempTop2Acc] {
    override def createAccumulator(): TempTop2Acc = new TempTop2Acc

    /**
     * 计算聚合结果的函数 accumulate
     *
     * @param acc
     * @param input
     */
    def accumulate(acc: TempTop2Acc, input: Double): Unit = {
        if (input > acc.highestTemp) {
            acc.secondHighestTemp = acc.highestTemp
            acc.highestTemp = input
        } else if (input > acc.secondHighestTemp) {
            acc.secondHighestTemp = input
        }
    }

    /**
     * 输出结果
     *
     * @param acc
     * @param out
     */
    def emitValue(acc: TempTop2Acc, out: Collector[(Double, Int)]): Unit = {
        if (acc.highestTemp == Double.MinValue) {
            acc.highestTemp = 0
        }
        if (acc.secondHighestTemp == Double.MinValue) {
            acc.secondHighestTemp = 0
        }
        out.collect(acc.highestTemp, 1)
        out.collect(acc.secondHighestTemp, 2)
    }
}
