package com.kunyandata.backtesting.task
import java.util
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil
import redis.clients.jedis.Tuple
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 跑指定日期（如：2016-09-21）以前的指定天数（如：5,7,10,14,15,20,30,60）的历史全量数据
  * Created by dcyang on 2016/9/21 0021.
  */
object HeatMeanAndStd {

  def valueAndScoreToMap(set: java.util.Set[Tuple]): mutable.Map[String, Double] = {

    val resultMap = mutable.Map[String, Double]()
    val iterator = set.iterator

    while (iterator.hasNext) {
      val valueAndScore = iterator.next()
      resultMap.put(valueAndScore.getElement, valueAndScore.getScore)
    }

    resultMap
  }

  def getMean(arrayBuffer: ArrayBuffer[Double]): Double = {
    val num: Int = arrayBuffer.size
    if (num == 0)  -1.0 else arrayBuffer.sum / num
  }

  def getStd(arrayBuffer: ArrayBuffer[Double]): Double = {

    val num = arrayBuffer.size

    if (num == 0 || num == 1) -1.0
    else {
      val mean = getMean(arrayBuffer)
      Math.sqrt(arrayBuffer.map(x => Math.pow(x - mean, 2)).sum / (num -1))
    }

  }

  def main(args: Array[String]) {

    val host = args(0)
    val port = args(1).toInt
    val auth = args(2)
    val db = args(3).toInt
    RedisHandler.init(host, port, auth, db)
    val jedis = RedisHandler.getInstance().getJedis

    val prefix = "count_heat_"

    // 指定开始日期
    val initData = args(4)

    // 指定需要计算的历史天数
    val offDay = args(5).toInt

    val endDate = CommonUtil.getDateStr("2016-01-06", offDay)
    val dayNums = CommonUtil.getOffset(initData, endDate)

    for(offDayNum <- 0 to dayNums){

      val date = CommonUtil.getDateStr(initData,-offDayNum)
      val key = prefix + date
      val start = Int.MinValue
      val end = Int.MaxValue
      val map = mutable.Map[String, ArrayBuffer[Double]]()
      val codes = jedis.zrange(key, start, end)

      val iterator = codes.iterator()

      while(iterator.hasNext){

        val code = iterator.next()
        map.put(code, new ArrayBuffer[Double]())
      }

      for (offset <- 1 to offDay) {

        val preKey = prefix + CommonUtil.getDateStr(date, -offset)
        val codesWithScores: util.Set[Tuple] = jedis.zrangeWithScores(preKey, start, end)
        val codeScoreMap = valueAndScoreToMap(codesWithScores)

        val iterator = codes.iterator()

        while(iterator.hasNext){

          val code = iterator.next()

          if (codeScoreMap.contains(code)) {
            map(code).+=(codeScoreMap(code))
          }

        }

      }

      val codeWithMean: mutable.Map[String, Double] = map.map(x => (x._1, getMean(x._2)))
      val codeWithStd = map.map(x => (x._1, getStd(x._2)))

      val outMeanKey = "heat_mean_" + offDay + "_" + date
      val meanIterator = codeWithMean.iterator

      while(meanIterator.hasNext){

        val codeAndMean: (String, Double) = meanIterator.next()
        val code = codeAndMean._1
        val mean = codeAndMean._2

        if(mean != -1.0){
          jedis.zadd(outMeanKey, mean, code)
        }

      }

      val outStdKey = "heat_std_" + offDay + "_" + date
      val stdIterator = codeWithStd.iterator

      while(stdIterator.hasNext){

        val codeAndStd: (String, Double) = stdIterator.next()
        val code = codeAndStd._1
        val std = codeAndStd._2

        if(std != -1.0){
          jedis.zadd(outStdKey, std, code)
        }

      }

      println(date + "\t" + offDay)

    }

  }
}
