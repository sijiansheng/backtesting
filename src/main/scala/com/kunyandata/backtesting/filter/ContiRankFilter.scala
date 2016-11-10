package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil

import scala.collection.mutable

/**
  * 连续N天排名超过M
  * Created by YangShuai
  * Created on 2016/8/24.
  */
class ContiRankFilter private(prefix: String, days: Int, start: Int, end: Int, endRank1: Int, startRank1: Int, endRank2: Int, startRank2: Int) extends Filter {

  override def filter(): List[String] = {

    val resultSet = mutable.Set[String]()
    val map = mutable.Map[String, Int]()
    val jedis = RedisHandler.getInstance().getJedis

    for (i <- start to end) {

      val key = prefix + CommonUtil.getDateStr(i)

      val stocks = jedis.zrevrange(key, 0, -1).toArray()

      val result = stocks.slice(startRank1 - 1, endRank1 - 1) ++ stocks.slice(startRank2 - 1, endRank2 - 1)

      map.foreach(x => {

        val key = x._1

        if (!result.contains(key))
          map.remove(key)

      })

      result.foreach(x => {

        val code = x.toString
        map.put(code, map.getOrElse(code, 0) + 1)

        if (map.getOrElse(code, 0) >= days)
          resultSet.add(code)

      })

    }

    jedis.close()

    resultSet.toList
  }

}

object ContiRankFilter {

  def apply(prefix: String, days: Int, start: Int, end: Int, endRank1: Int, startRank1: Int, endRank2: Int = 1, startRank2: Int = 1): ContiRankFilter = {

    val filter = new ContiRankFilter(prefix, days, start, end, endRank1, startRank1, endRank2, startRank2)

    filter.futureTask = new FutureTask[List[String]](new Callable[List[String]] {
      override def call(): List[String] = filter.filter()
    })

    filter
  }

}