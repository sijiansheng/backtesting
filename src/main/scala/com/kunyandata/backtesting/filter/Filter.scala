package com.kunyandata.backtesting.filter

import java.util.concurrent.FutureTask


/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
abstract class Filter {

  var futureTask: FutureTask[List[String]] = null

  def getFutureTask: FutureTask[List[String]] = futureTask

  def getResult: List[String] = futureTask.get()

  def filter(): List[String]

}
