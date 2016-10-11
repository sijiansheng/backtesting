[![Build Status](https://travis-ci.org/ysrotciv/backtesting.svg?branch=master)](https://travis-ci.org/ysrotciv/backtesting)

#数据准备
* 每日一组的数据（如行为数据，参照count_heat_yyyy-MM-dd）保存为zet，key为prefix_yyyy-MM-dd格式，value存股票代码字符串，score存对应的值。如果值为字符串形式，则使用hash来存。
* 只有一组的数据同上。

* 数据操作码范围定义

|时间要求|是否存在非表示时间的数值|示例|操作码区间|
|:---:|:---:|:---:|:---:|
|无（每日）|是|成交量等于4万|1-1000|
|每周|是|新闻访问热度每周大于X次|2000-4000|
|每月|是|新闻访问热度每月大于X次|4000-6000|
|每季|是|新闻访问热度每季大于X次|6000-8000|
|每年|是|新闻访问热度每年大于X次|8000-10000|
|连续X天|否|新闻趋势连续3天上涨|10000-15000|
|连续X天|是|连续1~3天被10个大V看好|15000-20000|
|无（累计）|否|国家队持股|40000-50000|
|无（累计）|是|诉讼仲裁10次以上|50000以上|

#数据列表
##只有一份的数据
|key|数据说明|
|:---:|:---:|
|total_equity|总股本|
| float_equity | 流通股本（万股）|
| market_value | 总市值 （万元）|
| liquid_market_value | 流通市值 （万元）|
| liquid_scale | 流通比例|
| top_stock_ratio | 十大股东持股比例  |
| holder_count | 股东户数 （户）|
| float_stock_num | 户均持股数 |
| change_in_holding | 增减持（1为增持，0为减持） |
| over_or_under_weight_holding | 增减持（高管增股，高管减持） |
| institution_stock_num | 机构持股 （万股）|

##每天一份的数据
|前缀|数据说明|
|:---:|:---:|
| change_percent | 涨跌幅，收益率|
| inc_change_percent | 涨幅 （> 0.0）|
| dec_change_percent | 跌幅 （< 0.0） |
| absolute_change_percent | 涨跌幅（绝对值） |
| amplitude | 振幅 |
| turnoverratio | 换手率 |
| volume | 成交量 （万股） |
| turnover | 成交额 （万元） |
| share_price | 股价 （元） |
