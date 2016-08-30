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
  val Exposure = Value(402, "exposure_")
  val ExposureWeek = Value(2402, "exposureWeek_")
  val ExposureMonth = Value(4402, "exposureMonth_")

  // 股票相关新闻访问量
  val Visit = Value(401, "visit_")
  val VisitWeek = Value(2401, "visitWeek_")
  val VisitMonth = Value(4401, "visitMonth_")

  // 大V看好看空
  val VipStockRise = Value(10005, "vipstockstatistic_rise_")
  val VipStockDown = Value(10006, "vipstockstatistic_down_")

  // 新闻情感
  val SentimentPos = Value(10003, "sentiment_")
  val SentimentNeg = Value(10004, "sentiment_")

  // 新闻访问量趋势
  val TrendRise = Value(10001, "trend_")
  val TrendDown = Value(10002, "trend_")

  // 事件
  val Events = Value(40002, "events_")
}
