package com.kunyandata.backtesting.logger

import org.apache.log4j.Logger

/**
  * Created by yangshuai on 2016/8/23.
  */
object BKLogger extends RootLogger {

  logger = Logger.getLogger("BACK_TESTING")
  switch = true

}
