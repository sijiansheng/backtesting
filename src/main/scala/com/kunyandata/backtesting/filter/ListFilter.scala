package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil
import com.kunyandata.backtesting.util.CommonUtil._
import com.kunyandata.backtesting.{FilterResult, SingleFilterResult, TempFilterResult}

import scala.collection.mutable.ListBuffer

/**
  * Created by sijiansheng on 2016/12/15.
  * 停牌x天以内
  * 复牌x天以内
  */
class ListFilter(prefix: String, listFlag: Int, min: Int, max: Int, start: Int, end: Int) extends Filter {

  def filter(): FilterResult = {

    val jedis = RedisHandler.getInstance().getJedis

    val redisKeys = new ListBuffer[String]()
    val dates = new ListBuffer[String]()

    for (i <- start to end) {
      dates += CommonUtil.getDateStr(i)
    }

    val stocksAndDate = dates.map { date =>
      (setToList(jedis.zrangeByScore(prefix + date, listFlag, listFlag)), date)
    }.toList

    getStockByQuantity(stocksAndDate, min, max)
  }

  def getStockByQuantity(stocksAndDate: List[(List[String], String)], min: Int, max: Int): FilterResult = {

    var result = new TempFilterResult()

    val tempMap = collection.mutable.Map[String, (Int, ListBuffer[SingleFilterResult])]()

    stocksAndDate.foreach { everyDayStocksAndDate =>

      val everyDayStocks = everyDayStocksAndDate._1
      val date = everyDayStocksAndDate._2

      everyDayStocks.foreach {

        stock =>

          if (tempMap.contains(stock)) {
            val count = tempMap(stock)._1 + 1
            var stockAndDate = tempMap(stock)._2
            stockAndDate.+=(Tuple2(stock, date))
            tempMap.put(stock, (count, stockAndDate))
          } else {
            tempMap.put(stock, (1, new ListBuffer[(String, String)]() += Tuple2(stock, date)))
          }

      }

    }

    tempMap.foreach { stockAndCount =>
      val count = stockAndCount._2._1
      val stockAndDate = stockAndCount._2._2

      if (count >= min && count <= max) {
        println(s"min:$min,max:$max,count:$count")
        result = result ++ stockAndDate
      }
    }

    result.toList
  }

}

object ListFilter {

  def apply(prefix: String, listFlag: Int, min: Int, max: Int, start: Int, end: Int): ListFilter = {

    val filter = new ListFilter(prefix, listFlag, min, max, start, end)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }

}