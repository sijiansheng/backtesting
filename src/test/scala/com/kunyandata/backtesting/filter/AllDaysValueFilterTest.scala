package com.kunyandata.backtesting.filter

import com.kunyandata.backtesting.config.Configuration
import com.kunyandata.backtesting.io.RedisHandler
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by sijiansheng on 2017/5/16.
  */
class ContiValueFilterTest extends FlatSpec with Matchers {

  def main(args: Array[String]) {

    it should "return a result and println" in {

      val path = "e://backtest/config.xml"

      val config = Configuration.getConfigurations(path)
      val redisMap = config._1
      RedisHandler.init(redisMap.get("ip").get, redisMap.get("port").get.toInt, redisMap.get("auth").get, redisMap.get("db").get.toInt)

      val prefix = "Trend_"
      //    val prefix = "industry_heat_"


      val result = ContiValueFilter(prefix, 2, 2, 2, -20, -5).filter()

      for (code <- result) {
        println(code)
      }

      println("#############################")
    }

  }
}
