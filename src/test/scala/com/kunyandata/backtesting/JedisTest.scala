package com.kunyandata.backtesting

import com.kunyandata.backtesting.config.Configuration
import com.kunyandata.backtesting.io.RedisHandler

import scala.collection.mutable

/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
object JedisTest extends App {

  val config = Configuration.getConfigurations(args(0))
  val redisMap = config._1
  RedisHandler.init(redisMap.get("ip").get, redisMap.get("port").get.toInt, redisMap.get("auth").get, redisMap.get("db").get.toInt)

  val jedis = RedisHandler.getInstance().getJedis
  val count = jedis.zcard("heat_2016-07-21")
  val result = jedis.zrangeWithScores("heat_2016-07-21", 0, -1)
  val iterator = result.iterator()

  val map = mutable.Map[String, Int]()

  while (iterator.hasNext) {
    val pair = iterator.next()
    val code = pair.getElement
    val score = pair.getScore.toInt
    map += code -> score
  }

  println(map)

}
