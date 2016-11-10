package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 过滤出redis中的zset中
  * 在某段时间范围内连续 N 天 score 值超过 M 的股票代码
  * Created by YangShuai
  * Created on 2016/8/24.
  */
class ContiValueFilter private(prefix: String, days: Int, min: Double, max: Double, start: Int, end: Int) extends Filter {

  override def filter(): List[String] = {

    val jedis = RedisHandler.getInstance().getJedis

    val redisKeys = new ListBuffer[String]()

    for (i <- start to end) {
      redisKeys += (prefix + CommonUtil.getDateStr(i))
    }

    val result = ContiValueFilterUtil.getContiValue(jedis, redisKeys.toList, days, min, max)
    jedis.close()

    result
  }

}

object ContiValueFilter {

  def apply(prefix: String, days: Int, min: Double, max: Double, start: Int, end: Int): ContiValueFilter = {

    val filter = new ContiValueFilter(prefix, days, min, max, start, end)

    filter.futureTask = new FutureTask[List[String]](new Callable[List[String]] {
      override def call(): List[String] = filter.filter()
    })

    filter
  }

}

object ContiValueFilterUtil {

  def getContiValue(jedis: Jedis, redisKeys: List[String], days: Int, min: Double, max: Double): List[String] = {

    val resultSet = mutable.Set[String]()
    val map = mutable.Map[String, Int]()


    redisKeys.foreach(redisKey => {

      val result = jedis.zrangeByScore(redisKey, min, max)

      map.foreach(x => {

        val key = x._1

        if (!result.contains(key))
          map.remove(key)

      })

      val iterator = result.iterator()

      while (iterator.hasNext) {

        val code = iterator.next()
        map.put(code, map.getOrElse(code, 0) + 1)

        if (map.getOrElse(code, 0) >= days)
          resultSet.add(code)

      }

    })

    resultSet.toList
  }

}
