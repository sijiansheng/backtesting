package com.kunyandata.backtesting.util

import java.text.SimpleDateFormat

import scala.collection.mutable.ListBuffer

/**
  * Created by sijiansheng on 2016/11/10.
  */
object HourUtil {

  /**
    * 根据起止的日期小时得到中间所有的日期和时间间隔
    *
    * @param startTimestamp 开始时间戳
    * @param endTimestamp   结束时间 格式同上
    * @return 返回时间间隔内的所有小时 如：开始时间为2010101008  结束时间为2010101010  则返回的结果时List("2010101008","2010101009","2010101010")
    */
  def getAllHourByStartAndEnd(startTimestamp: Long, endTimestamp: Long): List[String] = {

    val result = ListBuffer[String]()

    for (time <- (startTimestamp / 1000 / 60 / 60) to (endTimestamp / 1000 / 60 / 60)) {
      result += getDateStringByTimestampWithHour(time * 1000 * 60 * 60)
    }

    result.toList
  }

  def getTimestampByDateWithHour(dateWithHour: String): Long = new SimpleDateFormat("yyyyMMddHH").parse(dateWithHour).getTime

  def getDateStringByTimestampWithHour(timestamp: Long): String = new SimpleDateFormat("yyyy-MM-dd-HH").format(timestamp)

  def getRedisKeys(startTime: Long, endTime: Long, redisPrefix: String): List[String] = getAllHourByStartAndEnd(startTime, endTime).map(redisPrefix + _)

  /**
    * 通过起止日期的偏移量获得rediskey 主要是在reidis中按小时统计的表中使用
 *
    * @param startOffset 开始日期偏移量
    * @param endOffset 结束日期偏移量
    * @param redisPrefix redis前缀
    * @return redis key的集合
    */
  def getRedisKeys(startOffset: Int, endOffset: Int, redisPrefix: String): List[String] = {

    val startTimeStamp = CommonUtil.getTimeStampByOffset(startOffset)
    val endTimeStamp = CommonUtil.getTimeStampByOffset(endOffset)
    getRedisKeys(startTimeStamp, endTimeStamp, redisPrefix)
  }

}
