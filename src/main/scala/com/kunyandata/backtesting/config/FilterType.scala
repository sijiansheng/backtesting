package com.kunyandata.backtesting.config

/**
  * Created by YangShuai
  * Created on 2016/8/24.
  */
object FilterType extends Enumeration {

  val TotalEquity= Value(101,"total_equity") //总股本
  val FloatEquity=Value(102,"float_equity") //流通股本
  val MarketValue = Value(103,"market_value")//总市值
  val LiquidMarketValue= Value(104,"liquid_market_value")//流通市值
  val LiquidScale = Value(105,"liquid_scale") //流通比例
  val TopStockRatio = Value(106,"top_stock_ratio")//十大股东持股比例
  val HolderCount= Value(107,"holder_count")//股东户数
  val FloatStockNum = Value(108,"float_stock_num")//户均持股数
  val AbsoluteChangePercent = Value(201,"absolute_change_percent_")//涨跌幅（绝对值）
  val IncChangePercent = Value(202,"inc_change_percent_")//涨幅
  val DecChangePercent = Value(203,"dec_change_percent_")//跌幅
  val Amplitude =Value(204,"amplitude_")//振幅
  val TurnoverRation = Value(205,"turnoverratio_")//换手率
  val Volume = Value(206,"volume_")//成交量
  val Turnover = Value(207,"turnover_")//成交额
  val SharePrice = Value(208,"share_price_")//股价
  val ChangePercent = Value(209,"change_percent_")//涨跌幅，收益率

  // 股票相关新闻访问量和股票相关新闻转载量
  val Visit = Value(401, "visit_")
  val Exposure = Value(402, "exposure_")
  val VisitWeek = Value(2401, "visitWeek_")
  val ExposureWeek = Value(2402, "exposureWeek_")
  val VisitMonth = Value(4401, "visitMonth_")
  val ExposureMonth = Value(4402, "exposureMonth_")

  // 新闻访问量趋势
  val TrendRise = Value(10001, "trend_")
  val TrendDown = Value(10002, "trend_")

  // 新闻情感
  val SentimentPos = Value(10003, "sentiment_")
  val SentimentNeg = Value(10004, "sentiment_")

  // 大V看好看空
  val VipStockRise = Value(10005, "vipstockstatistic_rise_")
  val VipStockDown = Value(10006, "vipstockstatistic_down_")

  // 事件
  val Events = Value(40002, "events_")

  // 公告性事件
  val AnnouncementProfit = Value(50001, "announcement_profit_")
  val AnnouncementLawsuit = Value(50002, "announcement_lawsuit_")
  val AnnouncementIllegal = Value(50003, "announcement_illegal_")

  val ChangeInHolding = Value(60001,"change_in_holding")//增减持
  val InstitutionStockNum = Value(60002,"institution_stock_num")//机构持股

}
