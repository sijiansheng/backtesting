package com.kunyandata.backtesting

import java.util.concurrent.{ExecutorService, Executors}

import com.kunyandata.backtesting.config.Configuration
import com.kunyandata.backtesting.filter.common.{ContiRankFilter, ContiValueFilter}
import com.kunyandata.backtesting.io.{RedisHandler, KafkaProducerHandler, KafkaConsumerHandler}
import com.kunyandata.backtesting.logger.BKLogger

/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
object Scheduler {

  val threadPool: ExecutorService = Executors.newFixedThreadPool(20)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      BKLogger.error("未设置配置文件位置")
      return
    }

    val config = Configuration.getConfigurations(args(0))
    val redisMap = config._1
    val kafkaMap = config._2

    RedisHandler.init(redisMap.get("ip").get, redisMap.get("port").get.toInt, redisMap.get("auth").get, redisMap.get("db").get.toInt)

    val consumerHandler = KafkaConsumerHandler(kafkaMap.get("zookeeper").get, kafkaMap.get("groupId").get, kafkaMap.get("receiveTopic").get)
    val producerHandler = KafkaProducerHandler(kafkaMap.get("brokerList").get, kafkaMap.get("sendTopic").get)

    val streams = consumerHandler.getStreams

    for (stream <- streams) {

      val it = stream.iterator()

      while (it.hasNext()) {

        val message = new String(it.next().message())
        System.out.println(message)
        println(System.currentTimeMillis())
        val result = filter()
        println("result: " + result)
        producerHandler.sendMessage(result)
      }

    }

    RedisHandler.close()
    consumerHandler.shutdown()
    producerHandler.close()
  }

  def filter(): String = {

    val valueFilter = ContiValueFilter("count_heat_", 1000, 10, -10, -1)
    val diffFilter = ContiValueFilter("diff_heat_", 10, 3, -5, -1)
    val rankFilter = ContiRankFilter("count_heat_", 100, 3, -5, -1)

    threadPool.execute(valueFilter.getFutureTask)
    threadPool.execute(diffFilter.getFutureTask)
    threadPool.execute(rankFilter.getFutureTask)

    val valueList = valueFilter.getResult
    val diffList = diffFilter.getResult
    val rankList = rankFilter.getResult

    println("value size: " + valueList.size)
    println("diff size: " + diffList.size)
    println("rank size: " + rankList.size)

    valueList.intersect(diffList).intersect(rankList).mkString(",")
  }

}
