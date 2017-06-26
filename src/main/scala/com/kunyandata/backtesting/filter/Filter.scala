package com.kunyandata.backtesting.filter

import java.util.concurrent.FutureTask

import com.kunyandata.backtesting.FilterResult


/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
abstract class Filter {

  var futureTask: FutureTask[FilterResult] = null

  def getFutureTask: FutureTask[FilterResult] = futureTask

  def getResult: FilterResult = futureTask.get()

  def filter(): FilterResult

}
