package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.FilterResult
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.HourUtil
import com.kunyandata.backtesting.util.CommonUtil._

import scala.collection.mutable.ArrayBuffer

/**
  * 过滤出redis中的zset中
  * 在某段时间范围内连续 N 小时 score 值超过 M 的股票代码
  * Created by yangshuai
  * Created on 2016/8/24.
  */
class ContiValueFilterByHour private(prefix: String, criticalField: Int, min: Double, max: Double, startTime: String, endTime: String) extends Filter {

  override def filter(): FilterResult = {

    val jedis = RedisHandler.getInstance().getJedis
    val allTimes = HourUtil.getAllHourByStartAndEnd(startTime, endTime)

    allTimes.foreach(println)

    def getContiValueByHour(redisKey: String): Array[String] = {
      val filterResult = jedis.zrangeByScore(redisKey, min, max)
      val result = setToArray(filterResult)
      result
    }

    val result = FilterUtil.getUnionValueByDifferentWays(prefix, allTimes, criticalField, getContiValueByHour)

    jedis.close()

    result
  }

}

object ContiValueFilterByHour {

  def apply(prefix: String, criticalField: Int, min: Double, max: Double, startTime: String, endTime: String): ContiValueFilterByHour = {

    val filter = new ContiValueFilterByHour(prefix, criticalField, min, max, startTime, endTime)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }

}
