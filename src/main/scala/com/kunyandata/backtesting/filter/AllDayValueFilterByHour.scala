package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.HourUtil
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * 每小时均大（小）于XXX
  * Created by sijiansheng on 2016/11/10.
  */
class AllDayValueFilterByHour private(prefix: String, min: Double, max: Double, start: Int, end: Int) extends Filter {

  override def filter(): List[String] = {

    val jedis = RedisHandler.getInstance().getJedis
    val redisKeys = HourUtil.getRedisKeys(start, end, prefix)

    val result = AllDayValueFilterBuHourUtil.getValue(jedis, redisKeys, min, max)
    jedis.close()

    result
  }
}

object AllDayValueFilterByHour {

  def apply(prefix: String, min: Double, max: Double, start: Int, end: Int): AllDayValueFilterByHour = {

    val filter = new AllDayValueFilterByHour(prefix, min, max, start, end)

    filter.futureTask = new FutureTask[List[String]](new Callable[List[String]] {
      override def call(): List[String] = filter.filter()
    })

    filter
  }

}

object AllDayValueFilterBuHourUtil {

  def getValue(redis: Jedis, redisKeys: List[String], min: Double, max: Double): List[String] = {

    val resultSet = mutable.Set[String]()
    val jedis = RedisHandler.getInstance().getJedis
    var init = false

    redisKeys.foreach(redisKey => {

      val result = jedis.zrangeByScore(redisKey, min, max)

      if (!init) {

        val iterator = result.iterator()

        while (iterator.hasNext) {
          val code = iterator.next()
          resultSet.add(code)
        }

        init = true
      } else {

        val iterator = resultSet.iterator

        while (iterator.hasNext) {

          val code = iterator.next()

          if (!result.contains(code))
            resultSet.remove(code)

        }

      }

    })

    resultSet.toList
  }
}