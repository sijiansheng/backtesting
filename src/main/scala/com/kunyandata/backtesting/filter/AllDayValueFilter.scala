package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.{SingleFilterResult, FilterResult}
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 每日，每周或每月均大（小）于XXX
  * Created by YangShuai
  * Created on 2016/9/1.
  */
class AllDayValueFilter private(prefix: String, min: Double, max: Double, start: Int, end: Int) extends Filter {

  override def filter(): FilterResult = {

    val resultList = mutable.ListBuffer[SingleFilterResult]()

    val jedis = RedisHandler.getInstance().getJedis

    for (i <- start to end) {

      val date = CommonUtil.getDateStr(i)
      val key = prefix + date

      if (jedis.exists(key)) {

        val result = jedis.zrangeByScore(key, min, max)

        val iterator = result.iterator()

        while (iterator.hasNext) {
          val code = iterator.next()
          resultList += ((code, date))
        }

      }

    }

    jedis.close()

    resultList.toList
  }

}

object AllDayValueFilter {

  def apply(prefix: String, min: Double, max: Double, start: Int, end: Int): AllDayValueFilter = {

    val filter = new AllDayValueFilter(prefix, min, max, start, end)

    filter.futureTask = new FutureTask[FilterResult](new Callable[List[Tuple2[String, String]]] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }

}
