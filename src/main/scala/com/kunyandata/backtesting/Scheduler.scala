package com.kunyandata.backtesting

import com.kunyandata.backtesting.config.Configuration
import com.kunyandata.backtesting.io.KafkaConsumer
import com.kunyandata.backtesting.logger.BKLogger
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
object Scheduler {

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      BKLogger.error("未设置配置文件位置")
      return
    }

    val config = Configuration.getConfigurations(args(0))
    val redisMap = config._1
    val kafkaMap = config._2
    val receiveTopic = kafkaMap.get("receiveTopic").get

    val sparkConf = new SparkConf().setAppName("BACK_TESTING").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val kafkaConsumer = KafkaConsumer.getInstance(kafkaMap.get("zookeeper").get,
      kafkaMap.get("groupId").get, receiveTopic)

    val topicCountMap = Map(receiveTopic -> 1)
    val consumerMap = kafkaConsumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(receiveTopic).get

    for (stream <- streams) {
      println("enter for loop")
      val it = stream.iterator()
      println("after it")
      while (it.hasNext()) {
        println("enter while loop")
        val msg = new String(it.next().message())
        System.out.println(msg)
        println("end while")
      }
    }

  }

}
