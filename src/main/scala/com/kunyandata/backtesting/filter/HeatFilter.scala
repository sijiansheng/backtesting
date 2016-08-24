package com.kunyandata.backtesting.filter

import com.kunyandata.backtesting.io.RedisHandler
import org.apache.spark.rdd.RDD

/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
class HeatFilter extends Filter {

  override var data: RDD[String] = _

  override def loadData(days: Int, score: Float): Unit = {

    val jedis = RedisHandler.getJedis
  }

  override def result: RDD[String] = {
    null
  }

}
