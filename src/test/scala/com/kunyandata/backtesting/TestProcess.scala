package com.kunyandata.backtesting

import com.kunyandata.backtesting.config.Configuration
import com.kunyandata.backtesting.io.{KafkaProducerHandler, KafkaConsumerHandler, RedisHandler}
import com.kunyandata.backtesting.parser.Query
import org.scalatest.{Matchers, FlatSpec}
import play.api.libs.json.Json

import scala.collection.mutable

/**
  * Created by niujiaojiao on 2016/9/5.
  */
object TestProcess extends App {

  val config = Configuration.getConfigurations(args(0))
  val redisMap = config._1
  val kafkaMap = config._2
  RedisHandler.init(redisMap.get("ip").get, redisMap.get("port").get.toInt, redisMap.get("auth").get, redisMap.get("db").get.toInt)
  val consumerHandler = KafkaConsumerHandler(kafkaMap.get("zookeeper").get, kafkaMap.get("groupId").get, kafkaMap.get("receiveTopic").get)
  val producerHandler = KafkaProducerHandler(kafkaMap.get("brokerList").get, kafkaMap.get("sendTopic").get)


  val conditionshashOne = List(
    "1:总股本大于5000万小于5500万",
    "1:流通股本大于1000万小于1200万",
    "1:总市值大于50000亿小于100000亿",
    "1:流通市值大于20000亿小于30000亿",
    "1:股东户数大于5万小于20万",
    "1:流通比例大于3%小于4%",
    "1:十大股东持股比例大于2%小于4%",
    "1:户均持股数大于0万小于500万"

    // ,
    //    "新闻访问热度每天大于x次小于x次","新闻访问热度每周大于x次小于x次","新闻访问热度每月大于x次小于x次",
    //    "新闻访问热度每年大于x次小于x次","新闻转载热度每天大于x次小x次","新闻转载热度每周大于x次小于x次",
    //    "新闻转载热度每月大于x次小于x次","新闻转载热度每年大于x次小于x次",
    //    "盈利预增x%",
    //    "诉讼仲裁x次","违规处罚x次","盈利预增x%以上","诉讼仲裁x次以上","违规处罚x次以上","新闻趋势连续x天上涨",
    //    "新闻趋势连续x天下降","新闻趋势连续x天以上上涨","新闻趋势连续x天以上下降","新闻情感连续x天都是非负面情绪","新闻情感连续x天都是负面情绪",
    //    "新闻情感连续x天以上都是非负面情绪","新闻情感连续x天以上都是负面情绪",
    //    "连续x天被x个大V看好","连续x天被x个大V看空","连续x天以上被x个大V看好","连续x天以上被x个大V看空","连续x天被x个大V以上看好",
    //    "连续x天被x个大V以上看空","连续x~x天被x个大V看好","连续x~x天被x个大V看空","连续x天被x~x个大V看好",
    //    "连续x天被x~x个大V看空","查看热度连续x天上涨超过x","查看热度连续x天出现在topx","查看热度连续x天以上上涨超过x",
    //    "查看热度连续x天以上出现在topx","查看热度连续x天超过x","查看热度连续x天以上超过x"

  )
  val conditionshashTwo = List(
    "2:涨跌幅大于8%小于9%", "2:涨幅大于5%小于6%",
    "2:跌幅大于6%小于7%",
    "2:振幅大于8%小于9%", "2:换手率大于1%小于2%", "2:成交量大于60万小于80万",
    "2:股价大于11元小于12元", "2:收益率大于-7%小于-6%"
  ) //这个查询出来的条件都是空

  //  查询条件错误：收益率大于-7%小于-6%)

  //  var turnover = Query.parse("2:成交额大于x万小于x万")//由于0数量太多，目前还没找到合适的日期测试

  //  val temp = Scheduler.filter(turnover, "2015-06-12", "2015-06-12")
  val conditionshashThree = List(

    "4:新闻访问热度每天大于x次小于x次", "4:新闻访问热度每周大于x次小于x次", "4:新闻访问热度每月大于x次小于x次",
    "4:新闻访问热度每年大于x次小于x次", "4:新闻转载热度每天大于x次小x次", "4:新闻转载热度每周大于x次小于x次",
    "4:新闻转载热度每月大于x次小于x次", "4:新闻转载热度每年大于x次小于x次",
    "4:盈利预增x%",
    "4:诉讼仲裁x次", "4:违规处罚x次", "4:盈利预增x%以上", "4:诉讼仲裁x次以上", "4:违规处罚x次以上", "4:新闻趋势连续x天上涨",
    "4:新闻趋势连续x天下降", "4:新闻趋势连续x天以上上涨", "4:新闻趋势连续x天以上下降", "4:新闻情感连续x天都是非负面情绪", "4:新闻情感连续x天都是负面情绪",
    "4:新闻情感连续x天以上都是非负面情绪", "4:新闻情感连续x天以上都是负面情绪",
    "4:连续x天被x个大V看好", "4:连续x天被x个大V看空", "4:连续x天以上被x个大V看好", "4:连续x天以上被x个大V看空", "4:连续x天被x个大V以上看好",
    "4:连续x天被x个大V以上看空", "4:连续x~x天被x个大V看好", "4:连续x~x天被x个大V看空", "4:连续x天被x~x个大V看好",
    "4:连续x天被x~x个大V看空", "4:查看热度连续x天上涨超过x", "4:查看热度连续x天出现在topx", "4:查看热度连续x天以上上涨超过x",
    "4:查看热度连续x天以上出现在topx", "4:查看热度连续x天超过x", "4:查看热度连续x天以上超过x"
  )


  val resultsHashOne = List(
    "300501", "300508", "300372", "300372", "002803", "002558+6019939", "600868", "603726"
  )

  val resultsHashTwo = List(
    "002347", "600189", "300441", "300163", "600777", "300466", "600339", "300441"
  )

  for (index <- conditionshashTwo.indices) {

    var queryMap = Query.parse(conditionshashTwo(index))

    println("this is queryMap" + queryMap)
    val result = Scheduler.filter(queryMap, "2015-06-12", "2015-06-12")

    val stocks = result._1

    println("this is stocks" + stocks)
    //    println(results(index))
    //    assert(stocks.head == results(index))

  }



  consumerHandler.shutdown()
  producerHandler.close()


}
