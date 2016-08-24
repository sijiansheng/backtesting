package com.kunyandata.backtesting.filter

import org.apache.spark.rdd.RDD

/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
abstract class Filter {

  var data: RDD[String]

  def loadData(days: Int, score: Float)

  def result: RDD[String]
}
