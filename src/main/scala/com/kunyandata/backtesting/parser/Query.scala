package com.kunyandata.backtesting.parser

/**
  * Created by QQ on 2016/8/30.
  */
object Query {

  /**
    * 查询条件解析方法
    *
    * @param query 查询条件
    * @return
    */
  def parse(query: String): Map[Int, String] = {

    val queries = query.split("\\+")

    val resultTemp = queries.map(query => {

      parseByType(query)
    }).groupBy(_._1).map(x => (x._1, x._2.map(_._2).mkString(",")))

    resultTemp
  }

  /**
    * 将查询条件划分为不同的查询类别，分别进行处理
    * @param query 查询条件
    * @return
    */
  private def parseByType(query: String) = {

    // 获取条件码
    val conditionIndex = query.substring(0, 2)
    val conditionIndexNum = conditionIndex.replaceAll("\\:", "").toInt

    // 判断条件是查询语句还是事件名称
    if (conditionIndexNum < 200000) {

      Rules.template(query.replaceAll(conditionIndex, ""))
    } else if (conditionIndexNum > 200000) {

      (40003, query.replaceAll(conditionIndex, ""))
    } else {

      (-1, s"查询条件错误“$query”：条件不存在或存在非法字符")
    }
  }
}