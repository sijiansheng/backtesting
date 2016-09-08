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
  )
  val resultsHashOne = List("300501", "300508", "300372", "300372", "002803", "002558+6019939", "600868", "603726")
  val resultsHashTwo = List("002347", "600189", "300441", "300163", "600777", "300466", "600339", "300441")
  //for hashone and hashTwo

  val conditionshashTwo = List(
    "2:涨跌幅大于8.50%小于9%", "2:涨幅大于5%小于6%",
    "2:跌幅大于6%小于7%",
    "2:振幅大于8%小于9%", "2:换手率大于1%小于2%", "2:成交量大于60万小于80万",
    "2:股价大于11元小于12元", "2:收益率大于-7%小于-6%"
  ) //这个查询出来的条件都是空
  //  查询条件错误：收益率大于-7%小于-6%)
  //  var turnover = Query.parse("2:成交额大于x万小于x万")//由于0数量太多，目前还没找到合适的日期测试
  //  val temp = Scheduler.filter(turnover, "2015-06-12", "2015-06-12")


  for (index <- conditionshashOne.indices) {

    var queryMap = Query.parse(conditionshashOne(index))

    println("this is queryMap" + queryMap)
    val result = Scheduler.filter(queryMap, "2015-06-12", "2015-06-12")
    val stocks = result._1
    println("this is stocks" + stocks)
    assert(stocks.head == resultsHashOne(index))

  }

  val conditionshashThree = List()


  var queryMap = Query.parse("4:新闻访问热度每天大于77次小于80次")
  println(queryMap)
  var result = Scheduler.filter(queryMap, "2016-08-05", "2016-08-05")
  var stocks = result._1
  println(stocks)
  assert(stocks.head == "600327")

  queryMap = Query.parse("4:新闻访问热度每周大于112次小于114次")
  result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-01")
  stocks = result._1
  println(stocks)
  assert(stocks.head == "600999")

  queryMap = Query.parse("4:新闻访问热度每月大于292次小于293次")
  result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-01")
  stocks = result._1
  println(stocks)
  assert(stocks.head == "600499")

  queryMap = Query.parse("4:新闻转载热度每天大于38次小40次")
  result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-01")
  stocks = result._1
  assert(stocks.head == "600016")

  queryMap = Query.parse("4:新闻转载热度每周大于350次小于500次")
  result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-01")
  stocks = result._1
  assert(stocks.head == "300033")

  queryMap = Query.parse("4:新闻转载热度每月大于2000次小于4000次")
  result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-01")
  stocks = result._1
  assert(stocks.head == "300033")

  queryMap = Query.parse("4:新闻趋势连续2天下降")
  result = Scheduler.filter(queryMap, "2016-07-05", "2016-07-07")
  stocks = result._1
  println(stocks)
  var compares = List("300065", "002544", "601118", "600383", "600531", "603308", "600409",
    "600562", "300246", "601555", "600362", "600807", "600711", "600855", "600184", "601010",
    "601818", "603611", " 600100", "002025", "600115", "600340", "600889", "002679")
      assert(stocks.toList == compares)

  /*queryMap = Query.parse("4:新闻趋势连续5天以上上涨")
  result = Scheduler.filter(queryMap, "2016-07-05", "2016-07-10")
  stocks = result._1
  assert(stocks.head == "600048")

  queryMap = Query.parse("4:新闻趋势连续5天以上下降")
  result = Scheduler.filter(queryMap, "2016-07-05", "2016-07-10")
  stocks = result._1
  compares = List("600855", "600562")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:新闻情感连续7天都是负面情绪")
  result = Scheduler.filter(queryMap, "2016-07-05", "2016-07-15")
  stocks = result._1
  compares = List("300268", "600919", "000800", "300372")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:新闻情感连续7天以上都是负面情绪")
  result = Scheduler.filter(queryMap, "2016-07-05", "2016-07-15")
  stocks = result._1
  compares = List("300268", "600919", "000800", "300372")
  assert(stocks.toList == compares)*/

  /* queryMap = Query.parse("4:连续2天被1个大V看好")
   result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
   stocks = result._1
   println("1")
   println(stocks)
   compares = List("600149", "300529", "600837", "002329", "300166", "300015",
     "600895", "600758", "600606", "300331", "300218", "300347", "300339", "601028")
 //  assert(stocks.toList == compares)

   queryMap = Query.parse("4:连续2天被1个大V看空")
   result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
   stocks = result._1
   compares = List("600855", "600562")
   println("1")
   println(stocks)
   compares = List("002303", "601336", "600149", "300506", "002142", "002539", "600120",
     "603818", "300104", "002703", "600380", "600048", "603868", "600141")
   assert(stocks.toList == compares)

   queryMap = Query.parse("4:连续2天以上被1个大V看好")
   result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
   stocks = result._1
   compares = List("300268", "600919", "000800", "300372")
   println("1")
   println(stocks)
   compares = List("600149", "300529", "600837", "002329", "300166", "300015",
     "600895", "600758", "600606", "300331", "300218", "300347", "300339", "601028")
   assert(stocks.toList == compares)

   queryMap = Query.parse("4:连续2天以上被1个大V看空")
   result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
   stocks = result._1
   compares = List("300268", "600919", "000800", "300372")
   println("1")
   println(stocks)
   compares = List("002303", "601336", "600149", "300506", "002142", "002539",
     "600120", "603818", "300104", "002703", "600380", "600048", "603868", "600141")
   assert(stocks.toList == compares)*/

  /*queryMap = Query.parse("4:连续2天被1个大V以上看好")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("600149", "300529", "600837", "002329",
    "300166", "300015", "600895", "600758", "600606", "300331", "300218", "300347", "300339", "601028")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:连续2天被1个大V以上看空")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("002303", "601336", "600149", "300506",
    "002142", "002539", "600120", "603818", "300104", "002703", "600380", "600048", "603868", "600141")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:连续2~3天被1个大V看好")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("600149", "300529", "600837", "002329", "300166", "300015", "600895",
    "600758", "600606", "300331", "300218", "300347", "300339", "601028")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:连续2~3天被1个大V看空")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("002303", "601336", "600149", "300506", "002142", "002539", "600120",
    "603818", "300104", "002703", "600380", "600048", "603868", "600141")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:连续2天被1~2个大V看好")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("600149", "300529", "600837", "002142", "002329", "300166", "300015", "600895", "600758",
    "600606", "300331", "300218", "300347", "300339", "601028")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:连续2天被1~2个大V看空")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("002303", "601336", "600149", "300506", "002142", "002539", "600120", "603818", "300104", "002703", "600380", "600048", "603868", "600141")
  assert(stocks.toList == compares)*/


  /*queryMap = Query.parse("4:查看热度连续2天上涨超过100000")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("000002")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:查看热度连续2天以上上涨超过100000")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("000002")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:查看热度连续4天以上出现在top5")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("000002","300372")
  assert(stocks.toList == compares)
  //4
  queryMap = Query.parse("4:查看热度连续4天超过100000")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("000002","300372")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:查看热度连续5天以上超过100000")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("000002")
  assert(stocks.toList == compares)

  queryMap = Query.parse("4:查看热度连续4天出现在top5")
  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  stocks = result._1
  compares = List("000002","300372")
  assert(stocks.toList == compares)
  */


  //隔开
  //
  //  val queryMap = Query.parse("4:盈利预增1%")
  //  println(queryMap)
  //  val result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  //  val stocks = result._1
  //  println("1")
  //  var value = ""
  //  stocks.foreach(
  //    x => {
  //      value += x + ","
  //    }
  //  )
  //  println(value.stripSuffix(","))
  //  compares = List("000002")
  //  assert(stocks.toList == compares)

  //  queryMap = Query.parse("4:诉讼仲裁1次")
  //  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  //  stocks = result._1
  //  println("1")
  //  value = ""
  //  stocks.foreach(
  //    x => {
  //      value += x + ","
  //    }
  //  )
  //  println(value.stripSuffix(","))
  //  compares = List("000002")
  //  //  assert(stocks.toList == compares)
  //
  //  queryMap = Query.parse("4:违规处罚1次")
  //  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  //  stocks = result._1
  //  println("1")
  //  value = ""
  //  stocks.foreach(
  //    x => {
  //      value += x + ","
  //    }
  //  )
  //  println(value.stripSuffix(","))
  //  compares = List("000002","300372")
  //  //  assert(stocks.toList == compares)
  //  //4
  //  queryMap = Query.parse("4:盈利预增50%以上")
  //  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  //  stocks = result._1
  //  println("1")
  //  value = ""
  //  stocks.foreach(
  //    x => {
  //      value += x + ","
  //    }
  //  )
  //  println(value.stripSuffix(","))
  //  compares = List("000002","300372")
  //  //  assert(stocks.toList == compares)
  //
  //  queryMap = Query.parse("4:诉讼仲裁1次以上")
  //  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  //  stocks = result._1
  //  println("1")
  //  value = ""
  //  stocks.foreach(
  //    x => {
  //      value += x + ","
  //    }
  //  )
  //  println(value.stripSuffix(","))
  //  compares = List("000002")
  //  //  assert(stocks.toList == compares)
  //
  //  queryMap = Query.parse("4:违规处罚1次以上")
  //  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
  //  stocks = result._1
  //  println("1")
  //  value = ""
  //  stocks.foreach(
  //    x => {
  //      value += x + ","
  //    }
  //  )
  //  println(value.stripSuffix(","))
  //  compares = List("000002","300372")
  //  assert(stocks.toList == compares)


  consumerHandler.shutdown()
  producerHandler.close()


}
