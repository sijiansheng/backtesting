package com.kunyandata.backtesting.parser

import scala.collection.immutable.HashMap

/**
  * Created by QQ on 2016/8/30.
  */
object Rules {

  // 查询语句模板和对应的操作码
  val QUERYMAP = {

    HashMap(
      (1, HashMap(

        // 包含大于小于的语句的操作码
        ("总股本大于x万", 101),
        ("总股本小于x万", 101),
        ("总股本等于x万", 101),
        ("总股本大于x万小于x万", 101),
        ("流通股本大于x万", 102),
        ("流通股本小于x万", 102),
        ("流通股本等于x万", 102),
        ("流通股本大于x万小于x万", 102),
        ("总市值大于x亿", 103),
        ("总市值小于x亿", 103),
        ("总市值等于x亿", 103),
        ("总市值大于x亿小于x亿", 103),
        ("流通市值大于x亿", 104),
        ("流通市值小于x亿", 104),
        ("流通市值等于x亿", 104),
        ("流通市值大于x亿小于x亿", 104),
        ("流通比例大于x%", 105),
        ("流通比例小于x%", 105),
        ("流通比例等于x%", 105),
        ("流通比例大于x%小于x%", 105),
        ("十大股东持股比例大于x%", 106),
        ("十大股东持股比例小于x%", 106),
        ("十大股东持股比例等于x%", 106),
        ("十大股东持股比例大于x%小于x%", 106),
        ("股东户数大于x万", 107),
        ("股东户数小于x万", 107),
        ("股东户数等于x万", 107),
        ("股东户数大于x万小于x万", 107),
        ("户均持股数大于x万", 108),
        ("户均持股数小于x万", 108),
        ("户均持股数等于x万", 108),
        ("户均持股数大于x万小于x万", 108),
        ("机构持股数大于x万", 109),
        ("机构持股数小于x万", 109),
        ("机构持股数等于x万", 109),
        ("机构持股数大于x万小于x万", 109),

        // 非大于小于类型的数据操作码
        // 增减持
        //        ("大股东增股", 40001),
        //        ("大股东减股", 40001),
        ("高管增股", 40001),
        ("高管减股", 40001))),

      // 是否持股
      //        ("基金持股", 40002),
      //        ("券商持股", 40002),
      //        ("社保持股", 40002),
      //        ("信托持股", 40002),
      //        ("保险持股", 40002),
      //        ("QFII持股", 40002),
      //        ("国家队持股", 40002))),
      //        ("机构持股", 40002))),

      (2, HashMap(

        // 大于小于类型的查询条件
        // 涨跌幅
        ("涨跌幅大于x%", 201),
        ("涨跌幅小于x%", 201),
        ("涨跌幅等于x%", 201),
        ("涨跌幅大于x%小于x%", 201),
        ("涨幅大于x%", 202),
        ("涨幅小于x%", 202),
        ("涨幅等于x%", 202),
        ("涨幅大于x%小于x%", 202),
        ("跌幅大于x%", 203),
        ("跌幅小于x%", 203),
        ("跌幅等于x%", 203),
        ("跌幅大于x%小于x%", 203),

        //振幅
        ("振幅大于x%", 204),
        ("振幅小于x%", 204),
        ("振幅等于x%", 204),
        ("振幅大于x%小于x%", 204),

        //换手率
        ("换手率大于x%", 205),
        ("换手率小于x%", 205),
        ("换手率等于x%", 205),
        ("换手率大于x%小于x%", 205),

        //成交量
        ("成交量大于x万", 206),
        ("成交量小于x万", 206),
        ("成交量等于x万", 206),
        ("成交量大于x万小于x万", 206),

        //成交额
        ("成交额大于x万", 207),
        ("成交额小于x万", 207),
        ("成交额等于x万", 207),
        ("成交额大于x万小于x万", 207),

        //股价
        ("股价大于x元", 208),
        ("股价小于x元", 208),
        ("股价等于x元", 208),
        ("股价大于x元小于x元", 208),

        //收益率
        ("收益率大于x%", 209),
        ("收益率小于x%", 209),
        ("收益率等于x%", 209),
        ("收益率大于x小于x%", 209))),

      (3, HashMap(

        // 大于小于类型的查询条件
        //资金流入
        ("资金流入大于x万", 301),
        ("资金流入小于x万", 301),
        ("资金流入等于x万", 301),
        ("资金流入大于x万小于x万", 301))),

      (4, HashMap(

        // 因为事件名称没法提前确定，只能先设置一个key表示所有热点事件 40002

        // 大于小于类型的查询条件
        //新闻访问热度
        ("新闻访问热度每天大于x次", 401),
        ("新闻访问热度每天小于x次", 401),
        ("新闻访问热度每天等于x次", 401),
        ("新闻访问热度每天大于x次小于x次", 401),
        ("新闻访问热度每周大于x次", 2401),
        ("新闻访问热度每周小于x次", 2401),
        ("新闻访问热度每周等于x次", 2401),
        ("新闻访问热度每周大于x次小于x次", 2401),
        ("新闻访问热度每月大于x次", 4401),
        ("新闻访问热度每月小于x次", 4401),
        ("新闻访问热度每月等于x次", 4401),
        ("新闻访问热度每月大于x次小于x次", 4401),
        ("新闻访问热度每年大于x次", 8401),
        ("新闻访问热度每年小于x次", 8401),
        ("新闻访问热度每年等于x次", 8401),
        ("新闻访问热度每年大于x次小于x次", 8401),

        //新闻转载热度
        ("新闻转载热度每天大于x次", 402),
        ("新闻转载热度每天小于x次", 402),
        ("新闻转载热度每天等于x次", 402),
        ("新闻转载热度每天大于x次小x次", 402),
        ("新闻转载热度每周大于x次", 2402),
        ("新闻转载热度每周小于x次", 2402),
        ("新闻转载热度每周等于x次", 2402),
        ("新闻转载热度每周大于x次小于x次", 2402),
        ("新闻转载热度每月大于x次", 4402),
        ("新闻转载热度每月小于x次", 4402),
        ("新闻转载热度每月等于x次", 4402),
        ("新闻转载热度每月大于x次小于x次", 4402),
        ("新闻转载热度每年大于x次", 8402),
        ("新闻转载热度每年小于x次", 8402),
        ("新闻转载热度每年等于x次", 8402),
        ("新闻转载热度每年大于x次小于x次", 8402),

        //公告性事件
        ("盈利预增x%", 50001),
        ("诉讼仲裁x次", 50002),
        ("违规处罚x次", 50003),
        ("盈利预增x%以上", 50001),
        ("诉讼仲裁x次以上", 50002),
        ("违规处罚x次以上", 50003),

        //新闻趋势
        ("新闻趋势连续x天上涨", 10001),
        ("新闻趋势连续x天下降", 10002),
        ("新闻趋势连续x天以上上涨", 10001),
        ("新闻趋势连续x天以上下降", 10002),

        //新闻情感
        ("新闻情感连续x天都是非负面情绪", 10003),
        ("新闻情感连续x天都是负面情绪", 10004),
        ("新闻情感连续x天以上都是非负面情绪", 10003),
        ("新闻情感连续x天以上都是负面情绪", 10004),

        //大V观点
        ("连续x天被x个大V看好", 15001),
        ("连续x天被x个大V看空", 15002),
        ("连续x天以上被x个大V看好", 15001),
        ("连续x天以上被x个大V看空", 15002),
        ("连续x天被x个大V以上看好", 15001),
        ("连续x天被x个大V以上看空", 15002),
        ("连续x~x天被x个大V看好", 15001),
        ("连续x~x天被x个大V看空", 15002),
        ("连续x天被x~x个大V看好", 15001),
        ("连续x天被x~x个大V看空", 15002),

        //行为数据
        ("查看热度连续x天上涨超过x", 15003),
        ("查看热度连续x天出现在topx", 15004),
        ("查看热度连续x天以上上涨超过x", 15003),
        ("查看热度连续x天以上出现在topx", 15004),
        ("查看热度连续x天超过x", 15005),
        ("查看热度连续x天以上超过x", 15005))),
      (5, HashMap(("事件", 40003)))
    )
  }

  /**
    * 判断语句中的大于小于数值范围
    *
    * @param query 查询条件
    * @param typ   查询条件类型
    * @return 返回语句对应的操作码和条件
    * @author qiuqiu
    * @note rowNum:24
    */
  def biggerAndSmaller(query: String, typ: Int): (Int, String) = {

    // 获取该类别下的条件操作码
    val opCode = this.QUERYMAP(typ)

    // 判断query是否存在于条件模板中
    val keyNum = opCode.getOrElse(query.replaceAll("\\d+", "x"), -1)

    if (keyNum == -1) {

      // 如果key为-1，则认为该query不存在条件模板库中，直接返回该query
      (keyNum, s"查询条件错误：$query")
    }
    else if (keyNum < 10000) {

      // 如果keyNum小于1000，说明该条件为大于小于类型的查询条件
      // 生成获取数字的正则
      val regex = """\d+""".r
      val percent = query.contains("%")
      val value = regex.findAllIn(query).toArray

      if (value.length == 2 && value(0).toLong < value(1).toLong) {

        if (percent) {

          (keyNum, value.map(x => x.toDouble / 100).mkString(","))
        } else {

          (keyNum, value.mkString(","))
        }
      } else if (value.length == 1 && query.contains("大于")) {

        val max = Int.MaxValue
        if (percent) {

          val min = value.map(x => x.toDouble / 100).apply(0)
          (keyNum, s"$min,$max")
        } else {

          (keyNum, s"${value(0)},${Int.MaxValue}")
        }
      } else if (value.length == 1 && query.contains("小于")) {

        val min = Int.MinValue
        if (percent) {

          val max = value.map(x => x.toDouble / 100).apply(0)
          (keyNum, s"$min,$max")
        } else {

          val max = value(0)
          (keyNum, s"$min,$max")
        }
      } else if (value.length == 1 && query.contains("等于")) {

        val min = value(0)
        val max = value(0)
        (keyNum, s"$min,$max")
      } else {

        (-1, s"查询条件错误：$query")
      }
    }
    else {

      (0, "")
    }
  }

  /**
    * 判断查询条件中的布尔类型
    *
    * @param query 查询条件
    * @param typ   查询条件类型
    * @return 返回语句对应的操作码和条件
    * @author qiuqiu
    * @note rowNum:24
    */
  def isOrNot(query: String, typ: Int): (Int, String) = {

    val opCode = this.QUERYMAP(typ)
    val regex = """\d+""".r
    val value = regex.findAllIn(query).toArray

    if (value.length == 0) {
      val keyNum = opCode.getOrElse(query, -1)

      if (keyNum == -1) {

        if (typ == 5) {

          (40003, query)
        }
        else {

          // 如果key为-1，则认为该query不存在条件模板库中，直接返回该query
          (keyNum, s"查询条件错误：$query")
        }
      }
      else if (typ == 1) {

        keyNum match {

          case 40001 => if (query.contains("增持")) {

            (keyNum, "1,1")
          } else if (query.contains("减持")) {

            (keyNum, "0,0")
          } else {

            (-1, s"查询条件错误：$query")
          }
          case 40002 => (keyNum, s"0,${Int.MaxValue}")
          case _ => (-1, s"查询条件错误：$query")
        }
      } else {

        (0, "")
      }
    } else {

      (0, "")
    }

  }

  /**
    * 判断查询条件中的连续型时间条件
    *
    * @param query 查询条件
    * @param typ   查询条件的类型
    * @return 返回语句对应的操作码和条件
    * @author qiuqiu
    * @note rowNum:57
    */
  def continuous(query: String, typ: Int): (Int, String) = {

    val opCode = this.QUERYMAP(typ)

    val queryTemp = query.replaceAll("\\d+", "x")
    val regex = """\d+""".r
    val value = regex.findAllIn(query).toArray

    // 获取具体的语句的操作码
    val keyNum = opCode.getOrElse(queryTemp, -1)

    if (keyNum == -1) {

      // 如果key为-1，则认为该query不存在条件模板库中，直接返回该query
      (keyNum, s"查询条件错误：$query")
    }
    else if (typ == 4) {

      queryTemp match {

        case "盈利预增x%" => (keyNum, s"${value(0)},${value(0)}")
        case "诉讼仲裁x次" => (keyNum, s"${value(0)},${value(0)}")
        case "违规处罚x次" => (keyNum, s"${value(0)},${value(0)}")
        case "盈利预增x%以上" => (keyNum, s"${value(0)},${Int.MaxValue}")
        case "诉讼仲裁x次以上" => (keyNum, s"${value(0)},${Int.MaxValue}")
        case "违规处罚x次以上" => (keyNum, s"${value(0)},${Int.MaxValue}")
        case "新闻趋势连续x天上涨" => (keyNum, s"${value(0)},1,1")
        case "新闻趋势连续x天下降" => (keyNum, s"${value(0)},-1,-1")
        case "新闻趋势连续x天以上上涨" => (keyNum, s"${value(0)},1,1")
        case "新闻趋势连续x天以上下降" => (keyNum, s"${value(0)},-1,-1")
        case "新闻情感连续x天都是非负面情绪" => (keyNum, s"${value(0)},0,0.5")
        case "新闻情感连续x天都是负面情绪" => (keyNum, s"${value(0)},0.5,1")
        case "新闻情感连续x天以上都是非负面情绪" => (keyNum, s"${value(0)},0,0.5")
        case "新闻情感连续x天以上都是负面情绪" => (keyNum, s"${value(0)},0.5,1")
        case "连续x天被x个大V看好" => (keyNum, s"${value(0)},${value(1)},${value(1)}")
        case "连续x天被x个大V看空" => (keyNum, s"${value(0)},${value(1)},${value(1)}")
        case "连续x天以上被x个大V看好" => (keyNum, s"${value(0)},${value(1)},${value(1)}")
        case "连续x天以上被x个大V看空" => (keyNum, s"${value(0)},${value(1)},${value(1)}")
        case "连续x天被x个大V以上看好" => (keyNum, s"${value(0)},${value(1)},${value(1)}")
        case "连续x天被x个大V以上看空" => (keyNum, s"${value(0)},${value(1)},${value(1)}")
        case "连续x~x天被x个大V看好" => (keyNum, s"${value(0)},${value(2)},${value(2)}")
        case "连续x~x天被x个大V看空" => (keyNum, s"${value(0)},${value(2)},${value(2)}")

        case "连续x天被x~x个大V看好" =>
          if (value(1) < value(2)) {

            (keyNum, s"${value(0)},${value(1)},${value(2)}")
          } else {

            (-1, s"查询条件错误：$query")
          }

        case "连续x天被x~x个大V看空" =>
          if (value(1) < value(2)) {

            (keyNum, s"${value(0)},${value(1)},${value(2)}")
          } else {

            (-1, s"查询条件错误：$query")
          }

        case "查看热度连续x天上涨超过x" => (keyNum, s"${value(0)},${value(1)},${Int.MaxValue}")
        case "查看热度连续x天出现在topx" => (keyNum, s"${value(0)},1,${value(1)}")
        case "查看热度连续x天以上上涨超过x" => (keyNum, s"${value(0)},${value(1)},${Int.MaxValue}")
        case "查看热度连续x天以上出现在topx" => (keyNum, s"${value(0)},1,${value(1)}")
        case "查看热度连续x天超过x" => (keyNum, s"${value(0)},${value(1)},${Int.MaxValue}")
        case "查看热度连续x天以上超过x" => (keyNum, s"${value(0)},${value(1)},${Int.MaxValue}")
        case _ => (-1, s"查询条件错误：$query")
      }

    } else {

      (0, "")
    }
  }
}
