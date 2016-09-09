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


  //  val conditionsGroupOne = List(
  //    "1:总股本大于5000万小于5500万",
  //    "1:流通股本大于1000万小于1200万",
  //    "1:总市值大于50000亿小于100000亿",
  //    "1:流通市值大于20000亿小于30000亿",
  //    "1:股东户数大于5万小于20万",
  //    "1:十大股东持股比例大于2%小于4%",
  //    "1:户均持股数大于0万小于500万"
  //  )
  //  val resultsGroupOne = List("300501", "300508", "300372", "300372", "002803", "600868", "603726")
  //  for (index <- conditionsGroupOne.indices) {
  //
  //    var queryMap = Query.parse(conditionsGroupOne(index))
  //    val result = Scheduler.filter(queryMap, "2015-06-12", "2015-06-12")
  //    val stocks = result._1
  //    assert(stocks.head == resultsGroupOne(index))
  //
  //  }
  //
  //  val conditionGroupTwo = List(
  //    "2:涨跌幅大于8%小于9%",
  //    "2:跌幅大于6%小于7%",
  //    "2:振幅大于8%小于9%", "2:换手率大于1%小于2%", "2:成交量大于60万小于80万",
  //    "2:股价大于11元小于12元", "2:收益率大于-7%小于-6%"
  //  )
  //  val resultsGroupTwo = List("002347", "300441", "002044", "600777", "300466", "600339", "300441")
  //  for (index <- conditionGroupTwo.indices) {
  //    var queryMap = Query.parse(conditionGroupTwo(index))
  //    val result = Scheduler.filter(queryMap, "2015-06-12", "2015-06-12")
  //    val stocks = result._1
  //    assert(stocks.head == resultsGroupTwo(index))
  //  }

  var queryMap = Query.parse("1:流通比例大于3%小于4%")
  var result = Scheduler.filter(queryMap, "2016-08-05", "2016-08-05")
  var stocks = result._1
  assert(stocks.toList == List("002558", "601939"))

  //  queryMap = Query.parse("2:涨幅大于5.5%小于6.0%")
  //  result = Scheduler.filter(queryMap, "2015-06-12", "2015-06-12")
  //  stocks = result._1
  //  assert(stocks.head == "000876")
  //
  //  queryMap = Query.parse("2:成交额大于0万小于0.5万")
  //  result = Scheduler.filter(queryMap, "2015-06-12", "2015-06-12")
  //  stocks = result._1
  //  assert(stocks.toList == List("600777","000582","000519","300269","002313","002286","002515","000980","002191","002622","300331","600339","600654","002072","002492","601018","002608","000929","300220","002696","002544","300043","000657","000972","600271","600749","300142","300313","600168","000651","300362","000748","000023","002219","600745","000839","002089","000153","300292","002567","002349","002052","002044","002246","000415","000901","002409","002619","000876","600556","002040","600146","002347","002485","002018","002228","002075","002537","600822","300149","600643","002481","002652","600120","300046","300104","002638","000426","000693","002164","300407","000002","600530","002103","000836","601388","600759","002235","002082","000968","002389","002447","000511","600860","000962","000066","002612","600739","000925","002070","600539","300410","600502","000806","000711","300025","300169","600978","600576","002730","600455","002320","002207","000633","600189","600990","002514","300441","002201","300280","002555","000409","000571","002452","300466","002654","600873","600721","002065","300163","300299","300349","300052","300110","002174","002384","000553",
  //    "002691","600490","002434","002605","002636","300038","600847","600122","000796","600153","000638","600890"))
  //
  //
  //  queryMap = Query.parse("4:新闻访问热度每天大于77次小于80次")
  //   result = Scheduler.filter(queryMap, "2016-08-05", "2016-08-05")
  //   stocks = result._1
  //  assert(stocks.head == "600327")
  //
  //   queryMap = Query.parse("4:新闻访问热度每周大于112次小于114次")
  //   result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-01")
  //   stocks = result._1
  //   assert(stocks.head == "600999")
  //
  //   queryMap = Query.parse("4:新闻访问热度每月大于292次小于293次")
  //   result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-01")
  //   stocks = result._1
  //   assert(stocks.head == "600499")
  //
  //   queryMap = Query.parse("4:新闻转载热度每天大于38次小40次")
  //   result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-01")
  //   stocks = result._1
  //   assert(stocks.head == "600016")
  //
  //   queryMap = Query.parse("4:新闻转载热度每周大于350次小于500次")
  //   result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-01")
  //   stocks = result._1
  //   assert(stocks.head == "300033")
  //
  //   queryMap = Query.parse("4:新闻转载热度每月大于2000次小于4000次")
  //   result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-01")
  //   stocks = result._1
  //   assert(stocks.head == "300033")

//  queryMap = Query.parse("4:新闻趋势连续5天上涨")
//  result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-31")
//  stocks = result._1
//  assert(stocks.toList == List("002237", "000550", "600988", "600068", "000671", "600547",
//    "002264", "300024", "601186", "600663", "300386", "601390", "600089", "300340", "300298"))
//
//  queryMap = Query.parse("4:新闻趋势连续5天以上上涨")
//  result = Scheduler.filter(queryMap, "2016-07-05", "2016-07-10")
//  stocks = result._1
//  assert(stocks.head == "600048")
//
  queryMap = Query.parse("4:新闻情感连续7天都是负面情绪")
  result = Scheduler.filter(queryMap, "2016-07-05", "2016-07-15")
  stocks = result._1
  var compares = List("300268", "600919", "000800", "300372")
  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:新闻情感连续7天以上都是负面情绪")
//  result = Scheduler.filter(queryMap, "2016-07-05", "2016-07-15")
//  stocks = result._1
//  compares = List("300268", "600919", "000800", "300372")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:连续2天被1个大V看好")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("600149", "300529", "600837", "002329", "300166", "300015",
//    "600895", "600758", "600606", "300331", "300218", "300347", "300339", "601028")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:连续2天被1个大V看空")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("002303", "601336", "600149", "300506", "002142", "002539", "600120",
//    "603818", "300104", "002703", "600380", "600048", "603868", "600141")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:连续2天以上被1个大V看好")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("600149", "300529", "600837", "002329", "300166", "300015",
//    "600895", "600758", "600606", "300331", "300218", "300347", "300339", "601028")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:连续2天以上被1个大V看空")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("002303", "601336", "600149", "300506", "002142", "002539",
//    "600120", "603818", "300104", "002703", "600380", "600048", "603868", "600141")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:连续2天被1个大V以上看好")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  assert(stocks.toList == List("600149","300529","600837","002142","002329","300166",
//    "300015","600895","600758","600606","300331","300218","300347","300339","601028"))
//
//  queryMap = Query.parse("4:连续2天被1个大V以上看空")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("002303", "601336", "600149", "300506",
//    "002142", "002539", "600120", "603818", "300104", "002703", "600380", "600048", "603868", "600141")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:连续2~3天被1个大V看好")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("600149", "300529", "600837", "002329", "300166", "300015", "600895",
//    "600758", "600606", "300331", "300218", "300347", "300339", "601028")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:连续2~3天被1个大V看空")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("002303", "601336", "600149", "300506", "002142", "002539", "600120",
//    "603818", "300104", "002703", "600380", "600048", "603868", "600141")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:连续2天被1~2个大V看好")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("600149", "300529", "600837", "002142", "002329", "300166", "300015", "600895", "600758",
//    "600606", "300331", "300218", "300347", "300339", "601028")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:连续2天被1~2个大V看空")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("002303", "601336", "600149", "300506", "002142", "002539", "600120", "603818", "300104", "002703", "600380", "600048", "603868", "600141")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:查看热度连续2天上涨超过100000")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("000002")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:查看热度连续2天以上上涨超过100000")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("000002")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:查看热度连续4天以上出现在top5")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("000002", "300372")
//  assert(stocks.toList == compares)
//  //4
//  queryMap = Query.parse("4:查看热度连续4天超过100000")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("000002", "300372")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:查看热度连续5天以上超过100000")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("000002")
//  assert(stocks.toList == compares)
//
//  queryMap = Query.parse("4:查看热度连续4天出现在top5")
//  result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//  stocks = result._1
//  compares = List("000002", "300372")
//  assert(stocks.toList == compares)


    println("\n\n\n ok")
      queryMap = Query.parse("4:盈利预增100%")
      println(queryMap)
      result = Scheduler.filter(queryMap, "2016-08-01", "2016-08-26")
      stocks = result._1
      println("1")
      var value = ""
      stocks.foreach(
        x => {
          value += x + ","
        }
      )
      println(value.stripSuffix(","))
      compares = List("000002")
      assert(stocks.toList == compares)

//      queryMap = Query.parse("4:诉讼仲裁1次")
//      result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//      stocks = result._1
//      println("1")
//      value = ""
//      stocks.foreach(
//        x => {
//          value += x + ","
//        }
//      )
//      println(value.stripSuffix(","))
//      compares = List("000002")
//      //  assert(stocks.toList == compares)
//
//      queryMap = Query.parse("4:违规处罚1次")
//      result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//      stocks = result._1
//      println("1")
//      value = ""
//      stocks.foreach(
//        x => {
//          value += x + ","
//        }
//      )
//      println(value.stripSuffix(","))
//      compares = List("000002","300372")
//      //  assert(stocks.toList == compares)
//      //4
//      queryMap = Query.parse("4:盈利预增50%以上")
//      result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//      stocks = result._1
//      println("1")
//      value = ""
//      stocks.foreach(
//        x => {
//          value += x + ","
//        }
//      )
//      println(value.stripSuffix(","))
//      compares = List("000002","300372")
//      //  assert(stocks.toList == compares)
//
//      queryMap = Query.parse("4:诉讼仲裁1次以上")
//      result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//      stocks = result._1
//      println("1")
//      value = ""
//      stocks.foreach(
//        x => {
//          value += x + ","
//        }
//      )
//      println(value.stripSuffix(","))
//      compares = List("000002")
//  //      assert(stocks.toList == compares)
//
//      queryMap = Query.parse("4:违规处罚1次以上")
//      result = Scheduler.filter(queryMap, "2016-07-01", "2016-08-30")
//      stocks = result._1
//      println("1")
//      value = ""
//      stocks.foreach(
//        x => {
//          value += x + ","
//        }
//      )
//      println(value.stripSuffix(","))
//      compares = List("000002","300372")
//  //    assert(stocks.toList == compares)


  consumerHandler.shutdown()
  producerHandler.close()


}
