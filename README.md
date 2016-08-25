[![Build Status](https://travis-ci.org/ysrotciv/backtesting.svg?branch=master)](https://travis-ci.org/ysrotciv/backtesting)

#数据准备
##每日一组的数据（如行为数据，参照count_heat_yyyy-MM-dd）保存为zet，key为prefix_yyyy-MM-dd格式，value存股票代码字符串，score存对应的值。如果值为字符串形式，则使用hash来存。
##只有一组的数据同上。

# redis配置

## 连接配置：
### host: 61.147.114.72
### port: 6666
### auth: backtest

## 安装目录：
### path: /opt/redis-3.2.3 (72服务器)

## 启动方式：
### 启动：service redisd start
### 停止：service redisd stop
### 查看状态：service redisd status