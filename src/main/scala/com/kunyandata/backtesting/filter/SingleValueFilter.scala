package com.kunyandata.backtesting.filter

import java.io.FilterReader
import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting._
import com.kunyandata.backtesting.io.RedisHandler

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * 针对不需要每天（周，月）存一份的数据的过滤
  * 例如：总股本大于X亿
  * Created by YangShuai
  * Created on 2016/9/2.
  */
class SingleValueFilter private(prefix: String, min: Double, max: Double) extends Filter {

  override def filter(): FilterResult = {

    val key = prefix
    val resultList = mutable.ListBuffer[SingleFilterResult]()
    val jedis = RedisHandler.getInstance().getJedis
    val scoreResult = jedis.zrangeByScore(key, min, max)

    val iterator = scoreResult.iterator()

    while (iterator.hasNext) {
      val code = iterator.next()
      resultList += ((code, SINGLE_FLAG))
    }

    jedis.close()

    resultList.toList
  }

}

object SingleValueFilter {

  def apply(prefix: String, min: Double, max: Double): SingleValueFilter = {

    val filter = new SingleValueFilter(prefix, min, max)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }

}
