package com.kunyandata.backtesting.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ListBuffer
/**
  * Created by wangzhi on 2017/6/28.
  */
object DateUtil {

  val DATE_FORMAT = "yyyy-MM-dd"
  val DATE_FORMAT_MONTH = "yyyy-MM"

  /**
    * 根据起止的日期得到中间所有周一的日期
    *
    * @param startTimestamp 开始时间戳
    * @param endTimestamp   结束时间 格式同上
    * @return 返回时间间隔内的所有周一
    */
  def getAllWeek(startTimestamp: Long, endTimestamp: Long): List[String] = {

    val result =new ListBuffer[String]()

    val cal = Calendar.getInstance()

    for (time <- (startTimestamp / 1000 / 60 / 60 / 24 ) to (endTimestamp / 1000 / 60 / 60 / 24 )) {
      cal.setTimeInMillis(time * 1000 * 60 * 60 * 24)
      if(cal.get(Calendar.DAY_OF_WEEK) == 2){
        val date = new SimpleDateFormat(DATE_FORMAT).format(time * 1000 * 60 * 60 * 24)
        println(date)
        result += date
      }
    }

    result.toList
  }

  /**
    * 根据起止的日期得到中间所有周一的日期
    *
    * @param startTime 开始时间
    * @param endTime  结束时间 格式同上
    * @return 返回时间间隔内的所有月份
    */
  def getAllMonth(startTime: String, endTime: String): List[String] = {

    val result =new ListBuffer[String]()
    val sdf = new SimpleDateFormat(DATE_FORMAT_MONTH)
    val start = sdf.parse(startTime)
    val end = sdf.parse(endTime)
    val cal = Calendar.getInstance()
    cal.setTime(start)
    while(cal.getTime.before(end)){

      val date = sdf.format(cal.getTime)
      result += date+"-01"
      cal.add(Calendar.MONTH,1)


    }

    result += sdf.format(cal.getTime)+"-01"

    result.toList
  }

}
