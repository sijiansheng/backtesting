package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.{HourUtil, CommonUtil}

import scala.collection.mutable

/**
  * 过滤出redis中的zset中
  * 在某段时间范围内连续 N 天 score 值超过 M 的股票代码
  * Created by YangShuai
  * Created on 2016/8/24.
  */
class ContiValueFilterByHour private(prefix: String, criticalField: Int, min: Double, max: Double, start: Int, end: Int) extends Filter {

  override def filter(): List[String] = {

    val startTimeStamp = CommonUtil.getTimeStampByOffset(start)
    val endTimeStamp = CommonUtil.getTimeStampByOffset(end)

    val jedis = RedisHandler.getInstance().getJedis

    val redisKeys = HourUtil.getRedisKey(startTimeStamp,endTimeStamp,prefix)
    val result = ContiValueFilterUtil.getContiValue(jedis,redisKeys,criticalField,min,max)
    jedis.close()

    result
  }

}

object ContiValueFilterByHour {

  def apply(prefix: String, criticalField: Int, min: Double, max: Double, start: Int, end: Int): ContiValueFilterByHour = {

    val filter = new ContiValueFilterByHour(prefix, criticalField, min, max, start, end)

    filter.futureTask = new FutureTask[List[String]](new Callable[List[String]] {
      override def call(): List[String] = filter.filter()
    })

    filter
  }

}
