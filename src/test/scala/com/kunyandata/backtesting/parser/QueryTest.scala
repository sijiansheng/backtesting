package com.kunyandata.backtesting.parser

import org.scalatest._

/**
  * Created by YangShuai
  * Created on 2016/8/30.
  */
class QueryTest extends FlatSpec with Matchers {

  it should "change an option string to a map" in {

    val str = "1:总股本小于12亿+1:流通比例大于3%小于2%+1:户均持股数大于2万小于6万+1:大股东减股+2:涨跌幅大于1%小于2%+2:振幅等于12%+3:资金流入大于8万+4:连续5天被3~1个大V看好+4:查看热度连续7天出现在top10"
    val map = Query.parser(str)

    map.foreach(println)

  }

}
