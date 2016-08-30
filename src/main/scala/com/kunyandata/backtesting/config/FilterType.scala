package com.kunyandata.backtesting.config

/**
  * Created by YangShuai
  * Created on 2016/8/24.
  */
object FilterType extends Enumeration {

  val Example = Value(Int.MaxValue, "EXAMPLE")

  // 公告性事件
  val AnnouncementIllegal = Value(50003, "announcement_illegal_")
  val AnnouncementProfit = Value(50001, "announcement_profit_")
  val AnnouncementLawsuit = Value(50002, "announcement_lawsuit_")

  // 股票相关新闻转载量
  val exposure = Value(402, "exposure_")
  val exposureWeek = Value(2402, "exposureWeek_")
  val exposureMonth = Value(4402, "exposureMonth_")

  // 股票相关新闻访问量
  val visit = Value(401, "visit_")
  val visitWeek = Value(2401, "visitWeek_")
  val visitMonth = Value(4401, "visitMonth_")

  // 大V看好看空
  val vipStockRise = Value(10005, "vipstockstatistic_rise_")
  val vipStockDown = Value(10006, "vipstockstatistic_down_")

  // 新闻情感
  val sentimentPos = Value(10003, "sentiment_")
  val sentimentNeg = Value(10004, "sentiment_")

  val trendRise = Value(10001, "trend_")
  val trendDown = Value(10002, "trend_")
}
