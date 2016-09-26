package com.kunyandata.backtesting.task
import java.text.SimpleDateFormat
import java.util
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil
import redis.clients.jedis.Tuple
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
  * 计算历史热度均值和标准差
  * Created by dcyang on 2016/9/22 0022.
  */
object HeatMeanAndStdOneDay {

  /**
    * 将redis zrangewithscore 返回值转换成Map（value，score）
    *
    * @param set zrangewithscore 返回值
    * @return Map（value，score）
    * @author sijiansheng
    */
  def valueAndScoreToMap(set: java.util.Set[Tuple]): mutable.Map[String, Double] = {

    val resultMap = mutable.Map[String, Double]()
    val iterator = set.iterator

    while (iterator.hasNext) {
      val valueAndScore = iterator.next()
      resultMap.put(valueAndScore.getElement, valueAndScore.getScore)
    }

    resultMap
  }

  /**
    * 获取均值
    *
    * @param arrayBuffer  输入为热度的ArrayBuffer
    * @return 均值
    * @author dcyang
    */
  def getMean(arrayBuffer: ArrayBuffer[Double]): Double = {

    val num: Int = arrayBuffer.size

    if (num == 0)  -1.0
    else{
      val mean: Double = arrayBuffer.sum / num
      mean
    }
  }

  /**
    * 获取标准差
    *
    * @param arrayBuffer 输入为热度的ArrayBuffer
    * @return 标准差
    * @author dcyang
    */
  def getStd(arrayBuffer: ArrayBuffer[Double]): Double = {

    val num = arrayBuffer.size

    if (num == 0 || num == 1) -1.0
    else {

      val mean = getMean(arrayBuffer)
      val std: Double = Math.sqrt(arrayBuffer.map(x => Math.pow(x - mean, 2)).sum / (num -1))
      std
    }
  }

  def main(args: Array[String]) {

    // 初始化jedis
    val host = args(0)
    val port = args(1).toInt
    val auth = args(2)
    val db = args(3).toInt
    RedisHandler.init(host, port, auth, db)
    val jedis = RedisHandler.getInstance().getJedis

    val prefix = "count_heat_"

    // 获取前一天时间
    val DATE_FORMAT = "yyyy-MM-dd"
    val timeStamp = System.currentTimeMillis() - 24l * 60 * 60 * 1000
    val date = new SimpleDateFormat(DATE_FORMAT).format(timeStamp)

    val offDays = List(5, 7, 10, 14, 15, 20, 30, 60)
    val key = prefix + date

    // 处理不同的计算天数5、7、10、14、15、20、30、60
    for (offday <- offDays){

      val start = Int.MinValue
      val end = Int.MaxValue
      val map = mutable.Map[String, ArrayBuffer[Double]]()
      val codes = jedis.zrange(key, start, end)

      val iterator = codes.iterator()

      // 初始Map(股票，ArrayBuffer[热度])
      while(iterator.hasNext){

        val code = iterator.next()
        map.put(code, new ArrayBuffer[Double]())
      }

      for (offset <- 1 to offday) {

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

      // 计算并保存均值
      val codeWithMean: mutable.Map[String, Double] = map.map(x => (x._1, getMean(x._2)))
      val codeWithStd = map.map(x => (x._1, getStd(x._2)))

      val outMeanKey = "heat_mean_" + offday + "_" + date
      val meanIterator = codeWithMean.iterator

      while(meanIterator.hasNext){

        val codeAndMean: (String, Double) = meanIterator.next()
        val code = codeAndMean._1
        val mean = codeAndMean._2

        if(mean != -1.0){
          jedis.zadd(outMeanKey, mean, code)
        }

      }

      // 计算并保存标准差
      val outStdKey = "heat_std_" + offday + "_" + date
      val stdIterator = codeWithStd.iterator

      while(stdIterator.hasNext){

        val codeAndStd: (String, Double) = stdIterator.next()
        val code = codeAndStd._1
        val std = codeAndStd._2

        if(std != -1.0){
          jedis.zadd(outStdKey, std, code)
        }

      }

      println(date + offday)
    }
  }
}
