package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil

import scala.collection.mutable.ListBuffer

/**
  * Created by sijiansheng on 2016/12/15.
  * 停牌x天以内
  * 复牌x天以内
  */
class ListFilter(prefix: String, listFlag: Int, min: Int, max: Int, start: Int, end: Int) extends Filter {

  def filter(): List[String] = {

    val jedis = RedisHandler.getInstance().getJedis

    val redisKeys = new ListBuffer[String]()

    for (i <- start to end) {
      redisKeys += (prefix + CommonUtil.getDateStr(i))
    }

    val stocks = redisKeys.map { redisKey =>
      CommonUtil.setToList(jedis.zrangeByScore(redisKey, listFlag, listFlag))
    }.toList

    getStockByQuantity(stocks, min, max)
  }

  def getStockByQuantity(stocks: List[List[String]], min: Int, max: Int): List[String] = {

    val result = ListBuffer[String]()

    val tempMap = collection.mutable.Map[String, Int]()

    stocks.foreach { everyDayStocks =>

      everyDayStocks.foreach {

        stock =>
          if (tempMap.contains(stock)) {
            tempMap.put(stock, tempMap(stock) + 1)
          } else {
            tempMap.put(stock, 1)
          }

      }

    }
    tempMap.foreach { stockAndCount =>
      val count = stockAndCount._2
      val stock = stockAndCount._1
      if (count >= min && count <= max) {
        result += stock
      }
    }

    result.toList
  }

}

object ListFilter {

  def apply(prefix: String, listFlag: Int, min: Int, max: Int, start: Int, end: Int): ListFilter = {

    val filter = new ListFilter(prefix, listFlag, min, max, start, end)

    filter.futureTask = new FutureTask[List[String]](new Callable[List[String]] {
      override def call(): List[String] = filter.filter()
    })

    filter
  }


}