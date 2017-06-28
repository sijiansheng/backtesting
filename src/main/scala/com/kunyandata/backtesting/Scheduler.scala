package com.kunyandata.backtesting

import java.util.NoSuchElementException
import java.util.concurrent.{ExecutorService, Executors}

import com.kunyandata.backtesting.config.{Configuration, FilterType}
import com.kunyandata.backtesting.filter._
import com.kunyandata.backtesting.io.{KafkaConsumerHandler, KafkaProducerHandler, RedisHandler}
import com.kunyandata.backtesting.logger.BKLogger
import com.kunyandata.backtesting.parser.Query
import com.kunyandata.backtesting.util.CommonUtil
import com.kunyandata.backtesting.util.CommonUtil._
import kafka.consumer.KafkaStream
import play.api.libs.json.Json

import scala.collection.mutable.ListBuffer

/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
object Scheduler {

  val threadPool: ExecutorService = Executors.newFixedThreadPool(20)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      BKLogger.error("未设置配置文件位置")
      return
    }

    val config = Configuration.getConfigurations(args(0))
    val redisMap = config._1
    val kafkaMap = config._2

    RedisHandler.init(redisMap.get("ip").get, redisMap.get("port").get.toInt, redisMap.get("auth").get, redisMap.get("db").get.toInt)

    val consumerHandler = KafkaConsumerHandler(kafkaMap.get("zookeeper").get, kafkaMap.get("groupId").get, kafkaMap.get("receiveTopic").get)
    val producerHandler = KafkaProducerHandler(kafkaMap.get("brokerList").get, kafkaMap.get("sendTopic").get)

    val streams: List[KafkaStream[Array[Byte], Array[Byte]]] = consumerHandler.getStreams

    for (stream <- streams) {

      val it = stream.iterator()

      while (it.hasNext()) {

        try {

          val beginTime = System.currentTimeMillis()
          val message = new String(it.next().message())

          val jsonValue = Json.parse(message)
          println(message)

          val uid = (jsonValue \ "uid").as[Long]
          val session = (jsonValue \ "session").as[Long]
          val pushTime = (jsonValue \ "push_time").as[Long]
          val condition = (jsonValue \ "condition").as[String]
          val startDate = (jsonValue \ "start_time").as[String]
          val endDate = (jsonValue \ "end_time").as[String]
          val baseSession = (jsonValue \ "base_session").getOrElse(null)
          val search_type = (jsonValue \ "search_type").getOrElse(null)

          val queryMap = Query.parse(condition)

          val result = filter(queryMap, startDate, endDate)

          var rightOption = condition

          result._2.split(",").foreach(x => {
            rightOption = rightOption.replaceFirst(x, "")
          })

          val finishTime = System.currentTimeMillis()

          var resultValue = Json.obj(
            "uid" -> uid,
            "session" -> session,
            "push_time" -> pushTime,
            "start_time" -> startDate,
            "end_time" -> endDate,
            "stocks" -> result._1,
            "wrong_condition" -> result._2,
            "right_condition" -> rightOption,
            "begin_time_stamp" -> beginTime,
            "finish_time_stamp" -> finishTime,
            "cost_time" -> (finishTime - beginTime)
          )

          if (baseSession != null) {
            resultValue = resultValue ++ Json.obj("base_session" -> baseSession)
          }

          if (search_type != null && search_type.toString == "2") {
            resultValue = resultValue ++ Json.obj("search_type" -> search_type)
          }

          val sendMessage = Json.stringify(resultValue)

          BKLogger.warn(sendMessage)
          producerHandler.sendMessage(sendMessage)

        } catch {
          case e: Exception =>
          //            e.printStackTrace()
        }

      }

    }

    RedisHandler.close()
    consumerHandler.shutdown()
    producerHandler.close()
  }

  def filter(queryMap: Array[(Int, String)], startDate: String, endDate: String): (List[String], String) = {

    val wrongOption = queryMap.map { x => if (x._1 == -1) {
      x._2
    } else {
      ""
    }
    }.mkString
    val startOffset = CommonUtil.getOffset(startDate)
    val endOffset = CommonUtil.getOffset(endDate)

    var filters = ListBuffer[Filter]()
    var tempStockCodes = List[(String, String)]()

    println("queryMap.size:" + queryMap.length)

    queryMap.foreach(pair => {

      var prefix = ""
      var filterType = ""

      val key = pair._1

      println(s"key:$key")

      if (key > 0) {

        val values = pair._2.split(",")

        try {

          val infos = FilterType.apply(key).toString.split("\\|")
          prefix = infos(0)
          filterType = infos(1)

        } catch {

          case e: NoSuchElementException =>
          case e: IndexOutOfBoundsException =>
            e.printStackTrace()
        }

        println(s"prefix:$prefix,filterType:$filterType")
        println(s"startOffset:$startOffset,endOffset:$endOffset")
        println(s"startDate:$startDate,endDate:$endDate")

        filterType match {
          case "all_days_value" =>
            filters += AllDayValueFilter(prefix, values(0).toDouble, values(1).toDouble, startOffset, endOffset)
          case "conti_value" =>
            filters += ContiValueFilter(prefix, values(0).toDouble.toInt, values(1).toDouble, values(2).toDouble, startOffset, endOffset)
          case "conti_rank" =>
            filters += ContiRankFilter(prefix, values(0).toDouble.toInt, startOffset, endOffset, values(2).toDouble.toInt, values(1).toDouble.toInt)
          case "conti_value_hour" =>
            filters += ContiValueFilterByHour(prefix, values(0).toDouble.toInt, values(1).toDouble, values(2).toDouble, startDate, endDate)
          case "conti_value_week" =>
            filters += ContiValueFilterByWeek(prefix, values(0).toDouble.toInt, values(1).toDouble, values(2).toDouble, startOffset, endOffset)
          case "conti_value_month" =>
            filters += ContiValueFilterByMonth(prefix, values(0).toDouble.toInt, values(1).toDouble, values(2).toDouble, startDate, endDate)
          case "all_days_value_hour" =>
            filters += AllDayValueFilterByHour(prefix, values(0).toDouble, values(1).toDouble, startDate, endDate)
          case "conti_rank_false" =>
            filters += ContiRankFilter(prefix, values(0).toDouble.toInt, startOffset, endOffset, values(2).toDouble.toInt, values(1).toDouble.toInt, values(4).toDouble.toInt, values(3).toDouble.toInt)
          case "single_value" =>
            filters += SingleValueFilter(prefix, values(0).toDouble, values(1).toDouble)
          case "sum_value" =>
            filters += SumValueFilter(prefix, values(0).toDouble, values(1).toDouble, startOffset, endOffset)
          case "direct" =>
            filters += SimpleUnionFilter(prefix, values(0), startOffset, endOffset)
          case "simple" =>
            filters += SimpleFilter(prefix, values(0))
          case "standard_deviation" =>
            if (startOffset == endOffset) {
              filters += StandardDeviationFilter(prefix, values(0).toDouble, values(1).toDouble.toInt, values(2).toDouble.toInt, startOffset)
            } else {
              filters += VariousDateStandardDeviationFilter(prefix, values(0).toDouble, values(1).toDouble.toInt, values(2).toDouble.toInt, startOffset, endOffset)
            }
          case "standard_deviation_hour" =>
            filters += StandardDeviationFilterByHour(prefix, values(0).toDouble, values(1).toDouble.toInt, values(2).toDouble.toInt, startDate, endDate)
          case "list" =>
            filters += ListFilter(prefix, values(0).toDouble.toInt, values(1).toDouble.toInt, values(2).toDouble.toInt, startOffset, endOffset)
          case "market" =>
            filters += AppearToMarketFilter(prefix, values(0).toDouble.toLong, values(1).toDouble.toLong, startOffset, endOffset)
          case _ =>
            println("unknown")
        }

      }

    })

    filters.foreach(filter =>
      threadPool.execute(filter.getFutureTask)
    )

    println("filter.size：" + filters.size)

    //    filters.foreach(filter => println(s"查询条件的返回结果是：[${filter.getResult.mkString(",")}]"))

    for (i <- filters.indices) {

      if (i == 0) {
        tempStockCodes = filters(i).getResult
      } else {
        //        stockCodes = stockCodes.intersect(filters(i).getResult)
        tempStockCodes = getIntersection(tempStockCodes, filters(i).getResult)
      }
      println(s"筛选出的股票的数量：${tempStockCodes.size}")
    }

    tempStockCodes = tempStockCodes.take(3000)

    val result = filterResult2ListString(tempStockCodes)

    (result, wrongOption)
  }

}
