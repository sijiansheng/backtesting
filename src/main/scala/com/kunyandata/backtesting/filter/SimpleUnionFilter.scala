package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting._
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil

import scala.collection.mutable

/**
  * Created by yang
  * Created on 9/6/16.
  */
class SimpleUnionFilter private(prefix: String, event: String, start: Int, end: Int) extends Filter {

  override def filter(): FilterResult = {

    var resultSet = mutable.Set[String]()
    val jedis = RedisHandler.getInstance().getJedis

    for (i <- start to end) {

      val key = prefix + CommonUtil.getDateStr(i)
      val value = jedis.hget(key, event)

      if (value != null)
        resultSet ++= value.split(",").toSet
    }

    jedis.close()
    resultSet.map(x => (x, SINGLE_FLAG)).toList
  }

}

object SimpleUnionFilter {

  def apply(key: String, event: String, start: Int, end: Int): SimpleUnionFilter = {

    val filter = new SimpleUnionFilter(key, event, start, end)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }

}
