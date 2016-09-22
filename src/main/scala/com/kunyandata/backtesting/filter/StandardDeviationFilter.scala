package com.kunyandata.backtesting.filter

import java.util.concurrent.{Callable, FutureTask}

import com.kunyandata.backtesting.io.RedisHandler
import com.kunyandata.backtesting.util.CommonUtil
import redis.clients.jedis.Tuple

import scala.collection.mutable

/**
  * Created by sijiansheng on 2016/9/21.
  */
class StandardDeviationFilter private(prefix: String, multiple: Double, meanCriterion: Int, stdCriterion: Int, day: Int) extends Filter {

  override def filter(): List[String] = {

    var resultSet = mutable.Set[String]()
    val jedis = RedisHandler.getInstance().getJedis

    val date = CommonUtil.getDateStr(day)
    val key = prefix + date
    val valuesAndScores = jedis.zrangeByScoreWithScores(prefix + date, Double.MinValue, Double.MaxValue)
    val meanValuesAndScoresMap = valueAndScoreToMap(jedis.zrangeByScoreWithScores("heat_mean_" + meanCriterion + "_" + date, Double.MinValue, Double.MaxValue))
    val standardDeviationValuesAndScoresMap = valueAndScoreToMap(jedis.zrangeByScoreWithScores("heat_std_" + stdCriterion + "_" + date, Double.MinValue, Double.MaxValue))

    val iterator = valuesAndScores.iterator()

    while (iterator.hasNext) {

      val valueAndScore = iterator.next()
      val value = valueAndScore.getElement
      val score = valueAndScore.getScore
      val meanScore = meanValuesAndScoresMap.getOrElse(value, Double.MaxValue)
      val standardDeviationScore = standardDeviationValuesAndScoresMap.getOrElse(value, Double.MaxValue)

      if (meanScore != Double.MaxValue && standardDeviationScore != Double.MaxValue) {

        if (score.toDouble > (multiple * standardDeviationScore + meanScore)) {
          resultSet += value
        }

      }

    }


    jedis.close()
    resultSet.toList
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

object StandardDeviationFilter {

  def apply(prefix: String, multiple: Double, meanCriterion: Int, stdCriterion: Int, day: Int): StandardDeviationFilter = {

    val filter = new StandardDeviationFilter(prefix, multiple, meanCriterion, stdCriterion, day: Int)

    filter.futureTask = new FutureTask[List[String]](new Callable[List[String]] {
      override def call(): List[String] = filter.filter()
    })

    filter
  }
}
