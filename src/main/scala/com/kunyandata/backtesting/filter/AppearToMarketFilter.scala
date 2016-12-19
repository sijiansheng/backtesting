package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}
import java.util.function.LongFunction

import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.logger.BKLogger
import com.kunyandata.backtesting.util.CommonUtil

import scala.collection.mutable.ListBuffer

/**
  * Created by sijiansheng on 2016/12/15.
  * 上市x天以上
  *
  */
class AppearToMarketFilter(redisKey: String, minLimitedTime: Long, maxLimitedTime: Long, start: Int, end: Int) extends Filter {

  def filter(): List[String] = {

    val jedis = RedisHandler.getInstance().getJedis

    val results = new ListBuffer[String]()
    val stocksAndTime = jedis.zrangeByScoreWithScores(redisKey, Double.MinValue, Double.MaxValue)

    val iterator = stocksAndTime.iterator

    while (iterator.hasNext) {

      val valueAndScore = iterator.next()
      val listedTime = valueAndScore.getScore
      val flagTimeStamp = CommonUtil.getTimeStampByOffset(start) / 1000

      val judgeStandard = flagTimeStamp - listedTime
      if (judgeStandard >= minLimitedTime * 60 * 60 * 24L && judgeStandard < maxLimitedTime * 60 * 60 * 24L) {
        results += valueAndScore.getElement
      }

    }

    results.toList
  }

}

object AppearToMarketFilter {

  def apply(redisKey: String, min: Long, max: Long, start: Int, end: Int): AppearToMarketFilter = {

    val filter = new AppearToMarketFilter(redisKey, min, max, start, end)

    filter.futureTask = new FutureTask[List[String]](new Callable[List[String]] {
      override def call(): List[String] = filter.filter()
    })

    filter
  }


}