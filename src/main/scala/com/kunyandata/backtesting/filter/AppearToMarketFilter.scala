package com.kunyandata.backtesting.filter

import java.text.SimpleDateFormat
import java.util.concurrent.{Callable, FutureTask}
import com.kunyandata.backtesting.util.CommonUtil._

import com.kunyandata.backtesting.{TempFilterResult, FilterResult}
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.logger.BKLogger
import com.kunyandata.backtesting.util.CommonUtil

import scala.collection.mutable.ListBuffer

/**
  * Created by sijiansheng on 2016/12/15.
  * 上市x天以上
  * 上市x天
  * 上市x天以内
  */
class AppearToMarketFilter(redisKey: String, minLimitedTime: Long, maxLimitedTime: Long, start: Int, end: Int) extends Filter {

  def filter(): FilterResult = {

    val jedis = RedisHandler.getInstance().getJedis

    val results = new TempFilterResult()
    val stocksAndTime = jedis.zrangeByScoreWithScores(redisKey, Double.MinValue, Double.MaxValue)

    val iterator = stocksAndTime.iterator

    while (iterator.hasNext) {

      val valueAndScore = iterator.next()
      val marketDate = valueAndScore.getScore.toLong.toString

      if (marketDate.length == 8) {

        val listedTimeTamp = getTimeTampByDateFormat(DATE_FORMAT_OTHER, marketDate)
        val date = new SimpleDateFormat(DATE_FORMAT).format(listedTimeTamp)
        val flagTimeStamp = getTimeTampByDateFormat(DATE_FORMAT, new SimpleDateFormat(DATE_FORMAT).format(CommonUtil.getTimeStampByOffset(start)))
        val judgeStandard = (flagTimeStamp - listedTimeTamp) / (1000 * 60 * 60 * 24L)

        if (judgeStandard >= minLimitedTime && judgeStandard <= maxLimitedTime) {
          results += Tuple2(valueAndScore.getElement, date)
        }

      }

    }

    jedis.close()
    results.toList
  }

}

object AppearToMarketFilter {

  def apply(redisKey: String, min: Long, max: Long, start: Int, end: Int): AppearToMarketFilter = {

    val filter = new AppearToMarketFilter(redisKey, min, max, start, end)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }


}