package com.shuanghe.flink.api.sink

import com.shuanghe.flink.api.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
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

        //定义一个FlinkJedisConfigBase
        val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
            .setHost("localhost")
            .setPort(6379)
            .setPassword("123456")
            .build()
        dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))

        env.execute("redis sink")
    }
}

//redis实际操作
class MyRedisMapper extends RedisMapper[SensorReading] {
    //定义保存数据写入redis的命令
    //hset 表名 key value
    override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
    }

    override def getKeyFromData(data: SensorReading): String = {
        data.id
    }

    override def getValueFromData(data: SensorReading): String = {
        data.temperature.toString
    }
}
