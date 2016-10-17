package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil
import redis.clients.jedis.{Jedis, Tuple}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by sijiansheng on 2016/9/21.
  * 热度离均差过滤器和行业热度离均差过滤器
  * 日均查看热度离均差（MA30）大于 x 倍前30天日均热度标准差
  * prefix redis key前缀
  * ratio 比率 XX倍
  * meanValue 平均值计算标准，如30天平均值用30表示 5天平均值用5表示
  * standardDeviation 标准差计算标准，如30天标准差用30表示 5天标准差用5表示
  * day查询日期的偏移量
  */
class StandardDeviationFilter private(prefix: String, ratio: Double, meanValue: Int, standardDeviation: Int, day: Int) extends Filter {

  override def filter(): List[String] = {

    val jedis = RedisHandler.getInstance().getJedis
    val resultSet = StandardDeviationFilterUtil.getStcok(prefix, ratio, meanValue, standardDeviation, jedis, day)
    jedis.close()

    resultSet.toList
  }

}

object StandardDeviationFilter {

  def apply(prefix: String, multiple: Double, meanCriterion: Int, stdCriterion: Int, day: Int): StandardDeviationFilter = {

    val filter = new StandardDeviationFilter(prefix, multiple, meanCriterion, stdCriterion, day: Int)

    filter.futureTask = new FutureTask[List[String]](new Callable[List[String]] {
      override def call(): List[String] = filter.filter()
    })

    filter
  }
}

object StandardDeviationFilterUtil {

  def getStcok(prefix: String, ratio: Double, meanValue: Int, standardDeviation: Int, jedis: Jedis, day: Int): mutable.Set[String] = {

    var resultSet = mutable.Set[String]()
    var meanPrefix = ""
    var standardDeviationPrefix = ""

    if (prefix.contains("industry")) {
      meanPrefix = "industry_"
      standardDeviationPrefix = "industry_"
    }

    val date = CommonUtil.getDateStr(day)
    //获得的某天的所有的离均差
    val valuesAndScores = jedis.zrangeByScoreWithScores(prefix + date, Double.MinValue, Double.MaxValue)
    //获得的某天的所有的股票的平均值的map集合，key为股票代码，value为平均值
    val meanValuesAndScoresMap = valueAndScoreToMap(jedis.zrangeByScoreWithScores(meanPrefix + "heat_mean_" + meanValue + "_" + date, Double.MinValue, Double.MaxValue))
    //获得的某天的所有股票的标准差的map集合，key为股票代码，value为标准差
    val standardDeviationValuesAndScoresMap = valueAndScoreToMap(jedis.zrangeByScoreWithScores(standardDeviationPrefix + "heat_std_" + standardDeviation + "_" + date, Double.MinValue, Double.MaxValue))

    val iterator = valuesAndScores.iterator()

    while (iterator.hasNext) {

      val valueAndScore = iterator.next()
      val value = valueAndScore.getElement
      val score = valueAndScore.getScore
      val meanScore = meanValuesAndScoresMap.getOrElse(value, Double.MaxValue)
      val standardDeviationScore = standardDeviationValuesAndScoresMap.getOrElse(value, Double.MaxValue)

      if (meanScore != Double.MaxValue && standardDeviationScore != Double.MaxValue) {

        if (score.toDouble > (ratio * standardDeviationScore + meanScore)) {
          resultSet += value
        }

      }

    }

    resultSet
  }

  def valueAndScoreToMap(set: java.util.Set[Tuple]): mutable.Map[String, Double] = {

    val resultMap = mutable.Map[String, Double]()
    val iterator = set.iterator

    while (iterator.hasNext) {
      val valueAndScore = iterator.next()
      resultMap += (valueAndScore.getElement -> valueAndScore.getScore)
    }

    resultMap
  }
}
