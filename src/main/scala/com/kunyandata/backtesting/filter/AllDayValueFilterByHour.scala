package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting._
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.{CommonUtil, HourUtil}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * 每小时均大（小）于XXX
  * Created by sijiansheng on 2016/11/10.
  */
class AllDayValueFilterByHour private(prefix: String, min: Double, max: Double, startTime: String, endTime: String) extends Filter {

  override def filter(): FilterResult = {

    val resultList = mutable.ListBuffer[SingleFilterResult]()

    val jedis = RedisHandler.getInstance().getJedis

    val allTime = HourUtil.getAllHourByStartAndEnd(startTime, endTime)

    allTime.foreach {

      time =>

        val key = prefix + time

        if (jedis.exists(key)) {

          val result = jedis.zrangeByScore(key, min, max)

          val iterator = result.iterator()

          while (iterator.hasNext) {
            val code = iterator.next()
            resultList += ((code, time))
          }

        }
    }

    jedis.close()

    resultList.toList
  }
}

object AllDayValueFilterByHour {

  def apply(prefix: String, min: Double, max: Double, startTime: String, endTime: String): AllDayValueFilterByHour = {

    val filter = new AllDayValueFilterByHour(prefix, min, max, startTime, endTime)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }

}

