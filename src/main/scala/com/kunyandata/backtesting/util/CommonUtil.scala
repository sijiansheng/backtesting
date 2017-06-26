package com.kunyandata.backtesting.util

import java.text.{ParseException, SimpleDateFormat}

import com.kunyandata.backtesting._
import com.kunyandata.backtesting.logger.BKLogger

import scala.collection.mutable.ArrayBuffer


/**
  * Created by YangShuai
  * Created on 2016/8/24.
  */
object CommonUtil {

  val DATE_FORMAT = "yyyy-MM-dd"
  val HOUR_DATE_FORMAT = "yyyy-MM-dd-HH"
  val DATE_FORMAT_OTHER = "yyyyMMdd"

  /**
    * 获得yyyy-MM-dd格式的代表日期的字符串
    *
    * @param offset 偏差值，如果需获得昨天的日期字符串则传-1
    * @return
    */
  def getDateStr(offset: Int): String = {

    val timeStamp = System.currentTimeMillis() + offset * 24l * 60 * 60 * 1000

    new SimpleDateFormat(DATE_FORMAT).format(timeStamp)
  }

  /**
    * 获取距离指定天偏差天数的日期字符串
    *
    * @param date   指定日期字符串
    * @param offset 偏差天数
    * @return yyyy-MM-dd格式的代表日期的字符串
    */
  def getDateStr(date: String, offset: Int): String = {

    val timeStamp = new SimpleDateFormat(DATE_FORMAT).parse(date).getTime + offset * 24l * 60 * 60 * 1000

    new SimpleDateFormat(DATE_FORMAT).format(timeStamp)
  }


  def getTimeStampByOffset(offset: Int): Long = {
    System.currentTimeMillis() + offset * 24l * 60 * 60 * 1000
  }

  def getTimeTampByDateFormat(dateFormat: String, dateString: String): Long = {
    new SimpleDateFormat(dateFormat).parse(dateString).getTime
  }

  def getTimeStringByDateFormat(dateFormat: String, timeTamp: Long): String = new SimpleDateFormat(dateFormat).format(timeTamp)

  /**
    * 获取给定时间字符串的时间戳
    *
    * @param dateString 制定的时间字符串
    * @return 毫秒级的时间戳
    * @author QiuQiu
    */
  def getDateTimeStamp(dateString: String): Long = new SimpleDateFormat("yyyy-M-d-H").parse(dateString).getTime

  /**
    * 获得给定字符串所代表的日期与当日的偏差值
    * 例如：若给的是昨天则返回-1
    *
    * @param dateString 代表日期的字符串（"yyyy-MM-dd"）
    * @return 给定字符串所代表的日期与当日的偏差值
    */
  def getOffset(dateString: String): Int = {

    try {
      var timeStamp: Long = 0L

      if (dateString.length > 10) {
        timeStamp = new SimpleDateFormat(HOUR_DATE_FORMAT).parse(dateString).getTime
      } else {
        timeStamp = new SimpleDateFormat(DATE_FORMAT).parse(dateString).getTime
      }

      ((timeStamp - System.currentTimeMillis()) / (24l * 60 * 60 * 1000)).toInt
    } catch {

      case e: ParseException =>
        BKLogger.exception(e)

        -1
    }

  }

  /**
    * 获取两个指定日期相差的天数
    *
    * @param startDate 开始日期
    * @param endDate   结束日期
    * @return 相差天数
    */
  def getOffset(startDate: String, endDate: String): Int = {

    try {

      val startTimeStamp = new SimpleDateFormat(DATE_FORMAT).parse(startDate).getTime
      val endTimeStamp = new SimpleDateFormat(DATE_FORMAT).parse(endDate).getTime

      ((startTimeStamp - endTimeStamp) / (24l * 60 * 60 * 1000)).toInt
    } catch {

      case e: ParseException =>
        BKLogger.exception(e)

        -1
    }

  }

  def setToList[T <: String](set: java.util.Set[T]): List[T] = {

    val list = new collection.mutable.ListBuffer[T]()

    val iterator = set.iterator()

    while (iterator.hasNext) {
      list += iterator.next()
    }

    list.toList
  }

  def setToArray(set: java.util.Set[String]): Array[String] = {

    val arrayBuffer = new ArrayBuffer[String]()

    val iterator = set.iterator()

    while (iterator.hasNext) {
      arrayBuffer += iterator.next()
    }

    arrayBuffer.toArray
  }

  //求过滤的结果的交集
  def getIntersection(filterResult1: FilterResult, filterResult2: FilterResult): FilterResult = {


    if (filterResult1.nonEmpty && filterResult2.nonEmpty) {

      //如果结果1包含时间属性
      if (containTime(filterResult1)) {

        if (containTime(filterResult2)) {

          if (containHour(filterResult1)) {

            if (containHour(filterResult2)) {
              filterResult1.intersect(filterResult2)
            } else {
              intersectWithHourDateFormatAndDayDateFormat(filterResult2,filterResult1)
            }

          } else {

            if(containHour(filterResult2)){
              intersectWithHourDateFormatAndDayDateFormat(filterResult1,filterResult2)
            }else{
              filterResult1.intersect(filterResult2)
            }

          }

        } else {
          filterWithResult1HasTimeAndResult2NoTime(filterResult1, filterResult2)
        }

      } else {

        if (containTime(filterResult2)) {
          filterWithResult1HasTimeAndResult2NoTime(filterResult2, filterResult1)
        } else {
          filterResult1.intersect(filterResult2)
        }

      }


    } else {

      List()

    }
  }

  def intersectWithHourDateFormatAndDayDateFormat(hourDate: FilterResult, dayDate: FilterResult) = {
    hourDate.filter(x => {
      dayDate.contains((x._1, x._2.substring(0, 10)))
    })
  }

  def containTime(filterResult: FilterResult): Boolean = filterResult.head._2 != SINGLE_FLAG

  def filterResult2ListString(filterResult: FilterResult): List[String] = if (filterResult.isEmpty) List() else filterResult.map(x => s"${x._1}$FITER_RESULT_SEPARATOR${x._2}")


  def filterWithResult1HasTimeAndResult2NoTime(result1: FilterResult, result2: FilterResult): FilterResult = {

    val allNoTime = result2.map(_._1)
    result1.filter(x => allNoTime.contains(x._1))
  }

  def containHour(filterResult: FilterResult): Boolean = filterResult.head._2.length == 13

}
