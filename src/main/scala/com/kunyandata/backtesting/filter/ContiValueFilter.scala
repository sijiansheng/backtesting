package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.FilterResult
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil._

/**
  * 过滤出redis中的zset中
  * 在某段时间范围内连续 N 天 score 值超过 M 的股票代码
  * Created by YangShuai
  * Created on 2016/8/24.
  */
class ContiValueFilter private(prefix: String, days: Int, min: Double, max: Double, start: Int, end: Int) extends Filter {

  override def filter(): FilterResult = {

    val jedis = RedisHandler.getInstance().getJedis

    val dates = FilterUtil.getDays(start, end)

    def getContiValue(redisKey: String): Array[String] = {
      setToArray(jedis.zrangeByScore(redisKey, min, max))
    }

    val result = FilterUtil.getUnionValueByDifferentWays(prefix, dates, days, getContiValue)

    jedis.close()

    result
  }

}

object ContiValueFilter {

  def apply(prefix: String, days: Int, min: Double, max: Double, start: Int, end: Int): ContiValueFilter = {

    val filter = new ContiValueFilter(prefix, days, min, max, start, end)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }

}
