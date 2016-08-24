package com.kunyandata.backtesting.util

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by YangShuai
  * Created on 2016/8/24.
  */
object DateUtil {

  /**
    * 获得yyyy-MM-dd格式的代表日期的字符串
    * @param offset 偏差值，如果获得昨天的日期字符串写-1
    * @return
    */
  def getDateStr(offset: Int): String = {
    val timeStamp = System.currentTimeMillis() + offset * 24 * 60 * 60 * 1000
    new SimpleDateFormat("yyyy-MM-dd").format(timeStamp)
  }

}
