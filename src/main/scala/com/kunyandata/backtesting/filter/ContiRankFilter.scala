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
class ContiRankFilter private(prefix: String, days: Int, start: Int, end: Int, endRank1: Int, startRank1: Int = 0, startRank2: Int = 0, endRank2: Int = 0) extends Filter {

  override def filter(): List[String] = {

    getRank(prefix, days, start, end, endRank1, startRank1, startRank2, endRank2)
  }

  def getRank(prefix: String, days: Int, start: Int, end: Int, endRank1: Int, startRank1: Int = 0, startRank2: Int = 0, endRank2: Int = 0): List[String] = {

    val resultSet = mutable.Set[String]()
    val map = mutable.Map[String, Int]()
    val jedis = RedisHandler.getInstance().getJedis

    for (i <- start to end) {

      val key = prefix + CommonUtil.getDateStr(i)

      val stocks = jedis.zrevrange(key, 0, -1).toArray()

      val result = stocks.slice(startRank1, endRank1) ++ stocks.slice(startRank2, endRank2)

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

  def apply(prefix: String, days: Int, start: Int, end: Int, rank: Int): ContiRankFilter = {

    val filter = new ContiRankFilter(prefix, days, start, end, rank)

    filter.futureTask = new FutureTask[List[String]](new Callable[List[String]] {
      override def call(): List[String] = filter.filter()
    })

    filter
  }

}