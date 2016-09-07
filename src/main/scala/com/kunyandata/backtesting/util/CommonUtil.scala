package com.kunyandata.backtesting.util

import java.text.SimpleDateFormat

/**
  * Created by YangShuai
  * Created on 2016/8/24.
  */
object CommonUtil {

  val DATE_FORMAT = "yyyy-MM-dd"

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
    * 获得给定字符串所代表的日期与当日的偏差值
    * 例如：若给的是昨天则返回-1
    *
    * @param dateString 代表日期的字符串（"yyyy-MM-dd"）
    * @return 给定字符串所代表的日期与当日的偏差值
    */
  def getOffset(dateString: String): Int = {
    val timeStamp = new SimpleDateFormat(DATE_FORMAT).parse(dateString).getTime
    ((timeStamp - System.currentTimeMillis()) / (24l * 60 * 60 * 1000)).toInt
  }

}
