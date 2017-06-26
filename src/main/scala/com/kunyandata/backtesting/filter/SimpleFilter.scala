package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting._
import com.kunyandata.backtesting.io.RedisHandler

/**
  * Created by YangShuai
  * Created on 2016/9/12.
  */
class SimpleFilter private(key: String, field: String) extends Filter {

  override def filter(): FilterResult = {

    val jedis = RedisHandler.getInstance().getJedis
    val result = jedis.hget(key, field)

    jedis.close()
    result.split(",").map((_, SINGLE_FLAG)).toList
  }

}

object SimpleFilter {

  def apply(key: String, field: String): SimpleFilter = {

    val filter = new SimpleFilter(key, field)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }
}