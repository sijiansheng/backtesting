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
//  "1:总股本小于5500万","1:流通股本小于1200万","1:总市值小于100000亿","1:流通市值小于30000亿","1:股东户数小于20万"
//  "300501","300508","300372","300372","002803"
  val conditions = List(
  "1:总股本大于5000万小于5500万","1:流通股本大于1000万小于1200万","1:总市值大于50000亿小于100000亿","1:流通市值大于20000亿小于30000亿",
  "1:股东户数大于5万小于20万",
    "1:流通比例大于3%小于4%",
    "1:十大股东持股比例大于2%小于4%",
//    "户均持股数大于x万小于x万",
//    "涨跌幅大于x%小于x%","涨幅大于x%小于x%","跌幅大于x%小于x%","振幅大于x%小于x%","换手率大于x%小于x%","成交量大于x万小于x万",
//    "成交额大于x万小于x万","股价大于x元小于x元","收益率大于x小于x%","资金流入大于x万小于x万",
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

  val results= List(
    "300501","300508","300372","300372","002803","002558","600868"
  )

  for(index <-conditions.indices){

    var queryMap = Query.parse(conditions(index))

          println("this is queryMap"+queryMap)
    val result = Scheduler.filter(queryMap, "2016-07-21", "2016-08-21")

    val stocks = result._1

    println("this is stocks"+stocks)
//    println(results(index))
//    assert(stocks.head == results(index))


  }














//  val YiMap = Map(
//    "总市值"->"market_value",
//    "流通市值" -> "liquid_market_value"
//  )
//  val NumMap = Map(
//    "流通比例" -> "liquid_scale", "十大股东持股比例" -> "top_stock_ratio"
//  )
//
//  val WanMap = Map(
//    "总股本" -> "total_equity",
//    "流通股本" -> "float_equity",
//    "股东户数" -> "holder_count","户均持股数"->"float_stock_num"
//  )
//
//
//  val situationList = List("小于", "大于", "等于")
//
//  val wanCon = Tuple2("大于*小于",Tuple2("1000万","3000万"))
//  val YiCon = Tuple2("大于*小于",Tuple2("1000亿","3000亿"))
//  val NumCon = Tuple2("大于*小于",Tuple2("5%","9%"))
//
//  var YiCount = "30000亿"
//  val NumCount = "50%"
//  val WanCount = "3000万"
//
  val jedis = RedisHandler.getInstance().getJedis


//  callFunc(YiMap,situationList,YiCount)
//  callFunc(NumMap,situationList,NumCount)//还没调通
//  callFunc(WanMap,situationList,WanCount)



  consumerHandler.shutdown()
  producerHandler.close()
  jedis.close()



  def callFunc( map:Map[String,String],situationList: List[String],count :String ): Unit ={

    map.foreach(
      x => {
        val first = x._1
        val value = x._2
        for (i <- situationList.indices) {
          CorrectOrNot("1:", first, value, situationList(i), count)
        }
      }
    )
  }

  def CorrectOrNot(identifier: String, word: String, wordValue: String, sentence: String, count: String) = {

    println(identifier + word + sentence + count)

    try {

      val tempCnt = count

      var queryMap = Query.parse(identifier + word + sentence + count)

      println(queryMap)

      val result = Scheduler.filter(queryMap, "2016-07-21", "2016-08-21")

      val stocks = result._1
      val wrongCondition = result._2

      var left = 0.0
      var right = 0.0
      var temp = 0.0

      if(tempCnt.contains("亿")){
        temp = count.split("亿")(0).toDouble
      }else if(tempCnt.contains("万")){
        temp = count.split("万")(0).toDouble
      }else if (tempCnt.contains("%")) {
        temp = count.split("%")(0).toInt/100.toDouble
      }

      sentence match {

        case "大于" =>
          left = temp
          right = Integer.MAX_VALUE.toDouble
        case "等于" =>
          left = temp
          right = temp
        case "小于"=>
          left = Integer.MIN_VALUE.toDouble
          right = temp
      }

      println("left" + left)
      println("right"+ right)
      var rs = jedis.zrangeByScore(wordValue, left, right)
      val iterator = rs.iterator()

      //    assert(stocks.size == rs.size)

      println(stocks.size)
      println(rs.size)

      while (iterator.hasNext) {
        val code = iterator.next()
//        assert(stocks.contains(code))
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }



}
