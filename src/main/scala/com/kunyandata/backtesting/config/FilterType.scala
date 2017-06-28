package com.kunyandata.backtesting.config

/**
  * Created by YangShuai
  * Created on 2016/8/24.
  */
object FilterType extends Enumeration {

  val Error = Value(-1, "error")

  val TotalEquity= Value(101, "total_equity|single_value") // 总股本
  val FloatEquity=Value(102, "float_equity|single_value") // 流通股本
  val MarketValue = Value(103, "market_value|single_value") // 总市值
  val LiquidMarketValue= Value(104, "liquid_market_value|single_value") // 流通市值
  val LiquidScale = Value(105, "liquid_scale|single_value") // 流通比例
  val TopStockRatio = Value(106, "top_stock_ratio|single_value")// 十大股东持股比例
  val HolderCount= Value(107, "holder_count|single_value") // 股东户数
  val FloatStockNum = Value(108, "float_stock_num|single_value") // 户均持股数

  val InstitutionStockNum = Value(109, "institution_stock_num|single_value") // 机构持股数

  val AbsoluteChangePercent = Value(201, "absolute_change_percent_|all_days_value") // 涨跌幅（绝对值）
  val IncChangePercent = Value(202, "inc_change_percent_|all_days_value") // 涨幅
  val DecChangePercent = Value(203, "dec_change_percent_|all_days_value") // 跌幅
  val Amplitude =Value(204, "amplitude_|all_days_value") // 振幅
  val TurnoverRation = Value(205, "turnoverratio_|all_days_value") // 换手率
  val Volume = Value(206, "volume_|all_days_value") // 成交量
  val Turnover = Value(207, "turnover_|all_days_value") // 成交额
  val SharePrice = Value(208, "share_price_|all_days_value") // 股价
  val ChangePercent = Value(209, "change_percent_|all_days_value") // 涨跌幅，收益率
  val StandardDeviation = Value(210, "count_heat_|standard_deviation") //热度标准差
  val StandardDeviationByIndustry = Value(211, "industry_heat_|standard_deviation") //行业热度标准差
  val StandardDeviationWithHour = Value(212,"count_heat_hour_|standard_deviation_hour")
  val List = Value(213,"stock_traded_state_|list")
  val Market = Value(214,"stock_time_to_market_date|market")
//  val CapitalInflow = Value(301,)


  // 股票相关新闻访问量和股票相关新闻转载量
  val Visit = Value(401, "visit_|all_days_value") // 股票相关新闻的日访问量
  val VisitHour = Value(1401, "visitHour_|all_days_value_hour") // 股票相关新闻的小时访问量
  val Exposure = Value(402, "X|all_days_value") // 股票相关新闻的日转载量
  val ExposureHour = Value(1402, "exposureHour_|all_days_value_hour") // 股票相关新闻的小时转载量
  val VisitWeek = Value(2401, "visitWeek_|all_days_value") // 股票相关新闻的周访问量
  val ExposureWeek = Value(2402, "exposureWeek_|all_days_value") // 股票相关新闻的周转载量
  val VisitMonth = Value(4401, "visitMonth_|all_days_value") // 股票相关新闻的月访问量
  val ExposureMonth = Value(4402, "exposureMonth_|all_days_value") // 股票相关新闻的月转载量

  // 新闻访问量趋势
  val TrendHour = Value(10001, "trendHour_|conti_value_hour") // 股票相关新闻访问量小时趋势的增长/下跌
  val TrendDay = Value(10002, "trend_|conti_value") // 股票相关新闻访问量天趋势的增长/下跌
  val TrendWeek = Value(10003, "trendWeek_|conti_value_week") // 股票相关新闻访问量天趋势的增长/下跌
  val TrendMonth = Value(10004, "trendMonth_|conti_value_month") // 股票相关新闻访问量天趋势的增长/下跌

  // 新闻情感
  val SentimentPos = Value(10003, "sentiment_|conti_value") // 股票相关新闻非负面情绪
  val SentimentNeg = Value(10004, "sentiment_|conti_value") // 股票相关新闻负面情绪

  // 大V看好看空
  val VipStockRise = Value(15001, "vipstockstatistic_rise_|conti_value") // 大V看好
  val VipStockDown = Value(15002, "vipstockstatistic_down_|conti_value") // 大V看空

  //行为数据
  val HeatValueDiff = Value(15003, "diff_heat_|conti_value") //查看热度连续X天上涨大于X
  val HeatRank = Value(15004, "count_heat_|conti_rank") //查看热度连续X天出现在topX
  val HeatRankFalse = Value(15014, "count_heat_|conti_rank_false") //查看热度连续x天以上未出现在topx
  val HeatValue = Value(15005, "count_heat_|conti_value") //查看热度连续X天超过X
  val HeatHourValue = Value(16003, "diff_heat_hour_|conti_value_hour") //查看热度连续x小时以上上涨小于x
  val HeatValueByHour = Value(16005, "count_heat_hour_|conti_value_hour") //查看热度连续X小时大于X

  val ChangeInHolding = Value(40001,"over_or_under_weight_holding|simple") // 增减持
  // 事件
  val Events = Value(40003, "events_|direct") // 事件相关股票

  // 公告性事件
  val AnnouncementProfit = Value(50001, "announcement_profit_|sum_value") // 公告赢利预增（现在没有赢利预增的具体数值，只有次数）
  val AnnouncementLawsuit = Value(50002, "announcement_lawsuit_|sum_value") // 公告诉讼仲裁
  val AnnouncementIllegal = Value(50003, "announcement_illegal_|sum_value") // 公告违规处罚

}
