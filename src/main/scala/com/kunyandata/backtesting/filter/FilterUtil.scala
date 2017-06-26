package com.kunyandata.backtesting.filter

import com.kunyandata.backtesting._
import com.kunyandata.backtesting.util.CommonUtil

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by sijiansheng on 2017/5/16.
  */
object FilterUtil {

  //连续多少时间满足某种条件
  def getUnionValueByDifferentWays(redisPrefix: String, redisSuffixes: List[String], days: Int, obtainWays: String => Array[String]): FilterResult = {

    val result = new ListBuffer[SingleFilterResult]()

    //用来存储股票和对应日期的计数信息
    val stockMessagesWithCount = mutable.Map[(String, String), Int]()

    redisSuffixes.foreach {

      redisSuffix =>

        val arrayResult = obtainWays(redisPrefix + redisSuffix)
        var lastTime = redisSuffix

        //将yyyy-MM-dd-HH替换为yyyy-MM-dd HH:00
        if (redisSuffix.length == 13) {
          lastTime = s"${redisSuffix.substring(0, 10)} ${redisSuffix.substring(11, 13)}:00"
        }

        //把put放前面，遍历放后边，会导致，多累加一次，所以第一次put的数值为0，如果把判断放前面，put放后边，会出现等于1的这种条件过滤异常
        arrayResult.foreach(stock => {
          stockMessagesWithCount.put((stock, lastTime), 0)
        })

        stockMessagesWithCount.foreach {

          stockMessageWithCount =>

            val stockAndDate = stockMessageWithCount._1
            val count = stockMessageWithCount._2

            if (!arrayResult.contains(stockAndDate._1)) {
              stockMessagesWithCount.remove(stockAndDate)
            } else {

              val newCount = count + 1

              if (newCount == days) {
                result += stockAndDate
                stockMessagesWithCount.remove(stockAndDate)
              } else {
                stockMessagesWithCount.put(stockAndDate, newCount)
              }

            }

        }

    }

    result.toList
  }

  def getDays(startOffset: Int, endOffset: Int): List[String] = {

    val dates = new ListBuffer[String]()

    for (i <- startOffset to endOffset) {
      dates += CommonUtil.getDateStr(i)
    }

    dates.toList
  }

}
