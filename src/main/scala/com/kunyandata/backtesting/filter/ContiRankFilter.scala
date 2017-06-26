package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.FilterResult
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil._

import scala.collection.mutable

/**
  * 连续N天排名超过M
  * Created by YangShuai
  * Created on 2016/8/24.
  */
class ContiRankFilter private(prefix: String, days: Int, start: Int, end: Int, endRank1: Int, startRank1: Int, endRank2: Int, startRank2: Int) extends Filter {


  override def filter(): FilterResult = {

    val jedis = RedisHandler.getInstance().getJedis
    val dates = FilterUtil.getDays(start, end)

    def getContiRank(redisKey: String): Array[String] = {
      val stocks = setToArray(jedis.zrevrange(redisKey, 0, -1))
      stocks.slice(startRank1 - 1, endRank1 - 1) ++ stocks.slice(startRank2 - 1, endRank2 - 1)
    }

    val result = FilterUtil.getUnionValueByDifferentWays(prefix, dates, days, getContiRank)

    jedis.close()

    result
  }

}

object ContiRankFilter {

  def apply(prefix: String, days: Int, start: Int, end: Int, endRank1: Int, startRank1: Int, endRank2: Int = 1, startRank2: Int = 1): ContiRankFilter = {

    val filter = new ContiRankFilter(prefix, days, start, end, endRank1, startRank1, endRank2, startRank2)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }

}