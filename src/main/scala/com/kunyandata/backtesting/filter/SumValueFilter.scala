package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting
import com.kunyandata.backtesting.FilterResult
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil

import scala.collection.mutable

/**
  * 对给定日期范围内值得总和的过滤
  * Created by YangShuai
  * Created on 2016/9/2.
  */
class SumValueFilter private(prefix: String, min: Double, max: Double, start: Int, end: Int) extends Filter {

  override def filter(): FilterResult = {

    val scoreResult = mutable.ListBuffer[(String, String)]()
    val jedis = RedisHandler.getInstance().getJedis

    for (i <- start to end) {

      val timeFlag = CommonUtil.getDateStr(i)
      val key = prefix + timeFlag
      val result = jedis.zrangeByScore(key, min, max)

      val iterator = result.iterator()

      while (iterator.hasNext) {
        val code = iterator.next()
        scoreResult += ((code, timeFlag))
      }

    }

    jedis.close()

    scoreResult.toList
  }

}

object SumValueFilter {

  def apply(prefix: String, min: Double, max: Double, start: Int, end: Int): SumValueFilter = {

    val filter = new SumValueFilter(prefix, min, max, start, end)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }

}
