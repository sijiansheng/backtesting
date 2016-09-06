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

  val YiMap = Map(
    "总股本" -> "total_equity",
    "流通股本" -> "float_equity",
    "流通市值" -> "liquid_market_value"
  )
  val NumMap = Map(
    "流通比例" -> "liquid_scale", "十大股东持股比例" -> "top_stock_ratio"
  )

  val WanMap = Map(
    "股东户数" -> "holder_count","户均持股数"->"float_stock_num"
  )


  val situationList = List("小于", "大于", "等于")
  var YiCount = "30000亿"
  val NumCount = "0.2"
  val WanCount = "3000万"

  val jedis = RedisHandler.getInstance().getJedis


  callFunc(YiMap,situationList,YiCount)
  callFunc(NumMap,situationList,NumCount)
  callFunc(WanMap,situationList,WanCount)

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
    val queryMap = Query.parse(identifier + word + sentence + count)
    println(queryMap)
    val result = Scheduler.filter(queryMap, "2016-07-21", "2016-08-21")

    val stocks = result._1
    val wrongCondition = result._2


    var left = Int.MinValue
    var right = count.split("亿")(0).toInt

    sentence match {

      case "大于" =>
        left = count.split("亿")(0).toInt
        right = Int.MaxValue
      case "等于" =>
        left = count.split("亿")(0).toInt
        right = left
      case "小于"=> println("this is less than")
    }

    var rs = jedis.zrangeByScore(wordValue, left, right)
    val iterator = rs.iterator()

    assert(stocks.size == rs.size)

    try {

      while (iterator.hasNext) {
        val code = iterator.next()
        assert(stocks.contains(code))
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

}
