package scala.com.kunyandata.backtesting.task
import java.util
import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil
import redis.clients.jedis.Tuple
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by dcyang on 2016/9/21 0021.
  */
object HeatMeanAndStd {

  def valueAndScoreToMap(set: java.util.Set[Tuple]): mutable.Map[String, Double] = {

    val resultMap = mutable.Map[String, Double]()
    val iterator = set.iterator

    while (iterator.hasNext) {
      val valueAndScore = iterator.next()
      resultMap += (valueAndScore.getElement -> valueAndScore.getScore)
    }

    resultMap
  }

  def getMean(arrayBuffer: ArrayBuffer[Double]): Double = {

    val num: Int = arrayBuffer.size

    if (num == 0)  -1.0
    else{
      val mean: Double = arrayBuffer.sum / num
      mean
    }

  }

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

    val host = args(0)
    val port = args(1).toInt
    val auth = args(2)
    val db = args(3).toInt
    RedisHandler.init(host, port, auth, db)
    val jedis = RedisHandler.getInstance().getJedis

    val prefix = "count_heat_"

    // 指定开始日期
    var date = args(4)

    // 2016-02-04

    val offDays = 5
    var key = prefix + date
    val endDate = CommonUtil.getDateStr("2016-01-06", offDays)

    while(date != endDate){

      val start = Int.MinValue
      val end = Int.MaxValue
      val map = mutable.Map[String, ArrayBuffer[Double]]()
      val codes = jedis.zrange(key, start, end)

      val iterator = codes.iterator()
      while(iterator.hasNext){

        val code = iterator.next()
        map.put(code, new ArrayBuffer[Double]())
      }

      for (offset <- 1 to offDays) {

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

      val outMeanKey = "heat_mean_" + offDays + "_" + date
      val meanIterator = codeWithMean.iterator

      while(meanIterator.hasNext){

        val codeAndMean: (String, Double) = meanIterator.next()
        val code = codeAndMean._1
        val mean = codeAndMean._2
        if(mean != -1.0){
          jedis.zadd(outMeanKey, mean, code)
        }

      }

      val outStdKey = "heat_std_" + offDays + "_" + date
      val stdIterator = codeWithStd.iterator

      while(stdIterator.hasNext){

        val codeAndStd: (String, Double) = stdIterator.next()
        val code = codeAndStd._1
        val std = codeAndStd._2
        if(std != -1.0){
          jedis.zadd(outStdKey, std, code)
        }

      }

      println(date + "\t" + offDays)
      date = CommonUtil.getDateStr(date, -1)
      key = prefix + date

    }

  }
}
