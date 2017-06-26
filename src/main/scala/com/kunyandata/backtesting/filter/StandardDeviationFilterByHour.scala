package com.kunyandata.backtesting.filter

import java.text.SimpleDateFormat
import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting._
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.logger.BKLogger
import com.kunyandata.backtesting.util.{CommonUtil, HourUtil}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * redis中按小时求得的热度值之和和前一天标准差平均值的热度比较
  *
  * @param prefix            redis中热度值key的前缀，主要区别是否是产业热度值即是否包含industry
  * @param ratio             比率 倍数即标准差的乘数
  * @param meanValue         平均值标准 7 14 30等即7天平均值 14天平均值 30天平均值
  * @param standardDeviation 标准差标准 7，14 30同上
  */
class StandardDeviationFilterByHour private(prefix: String, ratio: Double, meanValue: Int, standardDeviation: Int, startTime: String, endTime: String) extends Filter {

  override def filter(): FilterResult = {

    val jedis = RedisHandler.getInstance().getJedis
    val times = HourUtil.getRedisKeys(getTimeStampByHourString(startTime), getTimeStampByHourString(endTime), "count_heat_hour_")
    //    BKLogger.warn(redisKeys.mkString(","))
    var meanPrefix = ""
    var standardDeviationPrefix = ""

    //区别产业热度和股票热度
    if (prefix.contains("industry")) {
      meanPrefix = "industry_"
      standardDeviationPrefix = "industry_"
    }

    //得到符合条件的股票热度之和，返回股票和热度的map集合
    val resultMap = StandardDeviationFilterByHour.getConditionalHeatMap(times, jedis)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis() - 1000 * 60 * 60 * 24)

    val resultSet = StandardDeviationFilterUtil.compareHeat(ratio, resultMap, jedis, meanPrefix, meanValue, standardDeviationPrefix, standardDeviation, date).map((_, SINGLE_FLAG))

    jedis.close()

    resultSet.toList
  }

  def getTimeStampByHourString(timeString: String): Long = {
    new SimpleDateFormat(CommonUtil.HOUR_DATE_FORMAT).parse(timeString).getTime
  }

}

object StandardDeviationFilterByHour {

  def apply(prefix: String, multiple: Double, meanCriterion: Int, stdCriterion: Int, startDay: String, endDay: String): StandardDeviationFilterByHour = {

    val filter = new StandardDeviationFilterByHour(prefix, multiple, meanCriterion, stdCriterion, startDay, endDay)

    filter.futureTask = new FutureTask[FilterResult](new Callable[FilterResult] {
      override def call(): FilterResult = filter.filter()
    })

    filter
  }


  /**
    * 从redis中得到股票和该股票在redis所有key的热度的和值
    *
    * @param redisKeys redis key
    * @param jedis     jedis连接
    * @return 股票和该股票在redis所有key的热度的和值
    */
  def getConditionalHeatMap(redisKeys: List[String], jedis: Jedis): mutable.Map[String, Double] = {

    val resultMap = mutable.Map[String, Double]()

    val redisData = redisKeys.map(redisKey => {
      StandardDeviationFilterUtil.valueAndScoreToMap(jedis.zrangeByScoreWithScores(redisKey, Double.MinValue, Double.MaxValue))
    })


    redisData.foreach(redisCell => {

      redisCell.foreach(

        stockAndHeat => {
          val stock = stockAndHeat._1
          val heat = stockAndHeat._2

          if (resultMap.contains(stock)) {
            val lastHeat = resultMap.getOrElse(stock, 0.0)
            resultMap.put(stock, lastHeat + heat)
          } else {
            resultMap.put(stock, heat)
          }

        }

      )

    })

    resultMap
  }

}
