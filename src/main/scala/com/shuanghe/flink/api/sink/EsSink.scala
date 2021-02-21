package com.shuanghe.flink.api.sink

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

object EsSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputPath = "C:\\Users\\yushu\\studyspace\\my_flink_study\\src\\main\\resources\\sensor.txt"
        val inputStream: DataStream[String] = env.readTextFile(inputPath)

        val dataStream: DataStream[SensorReading] = inputStream
            .map(data => {
                val arr = data.split("\t")
                SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
            })

        val httpHosts = new util.ArrayList[HttpHost]()
        httpHosts.add(new HttpHost("localhost", 9200))
        dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts, new MyEsSinkFunc).build())

        env.execute("es sink")
    }
}

class MyEsSinkFunc extends ElasticsearchSinkFunction[SensorReading] {
    override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        //包装一个Map作为data source
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("id", element.id)
        dataSource.put("temperature", element.temperature.toString)
        dataSource.put("timestamp", element.timestamp.toString)

        //创建indexRequest,用于发送http请求
        val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(dataSource)

        //用indexer发送请求
        indexer.add(indexRequest)
    }
}
