package com.wangbin.clouddemo.util
import com.wangbin.clouddemo.util.DateUtils
import com.wangbin.clouddemo.util.ToNum.parse2Date
object TestDate {

  def main(args: Array[String]): Unit = {

    val date = DateUtils.getLastDateOnMonth(DateUtils.getPreviousMonth(parse2Date("2019-02-28", "yyyy-MM-dd")))

    println(DateUtils.format(date,"yyyy-MM-dd"))
  }

}
