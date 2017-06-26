package com.kunyandata

import scala.collection.mutable.ListBuffer

/**
  * Created by sijiansheng on 2017/5/12.
  */
package object backtesting {

  type SingleFilterResult = Tuple2[String, String]
  type FilterResult = List[SingleFilterResult]
  type TempFilterResult = ListBuffer[SingleFilterResult]
  val SINGLE_FLAG = "single"
  val FITER_RESULT_SEPARATOR = "->"
}

