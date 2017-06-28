package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting._
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil._
import com.kunyandata.backtesting.util.DateUtil

/**
  * Created by wangzhi on 2017/6/28.
  */
class ContiValueFilterByMonth private(prefix: String, months: Int, min: Double, max: Double, start: String, end: String) extends Filter {

  override def filter(): FilterResult = {

    val jedis = RedisHandler.getInstance().getJedis

    val dates = DateUtil.getAllMonth(start, end)

    def getContiValue(redisKey: String): Array[String] = {
      setToArray(jedis.zrangeByScore(redisKey, min, max))
    }

    val result = FilterUtil.getUnionValueByDifferentWays(prefix, dates, months, getContiValue)

    jedis.close()

    result
  }

}

object ContiValueFilterByMonth {

  def apply(prefix: String, days: Int, min: Double, max: Double, start: String, end: String): ContiValueFilter = {

    val filter = new ContiValueFilter(prefix, days, min, max, start, end)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }

}

