package com.wangbin.clouddemo.util
import java.sql.Timestamp
import java.util.{Calendar, Date}

import com.wangbin.clouddemo.util.DateUtils
import com.wangbin.clouddemo.util.ToNum.parse2Date
import org.apache.commons.lang3.time.FastDateFormat
object TestDate {

  def main(args: Array[String]): Unit = {

    val date = DateUtils.getLastDateOnMonth(DateUtils.getPreviousMonth(parse2Date("2019-02-28", "yyyy-MM-dd")))

    println(DateUtils.format(date, "yyyy-MM-dd"))

    println(addMonth2Timestamp("20191202", "yyyyMMdd", 12))

    val range: String = DateUtils.rangeYMD(DateUtils.parse2Date("20190228"), DateUtils.parse2Date("20180121"))

    println("range:" + range)

    val c: Int = rangebohai("2019-02-01")
    println(c)
  }

    def rangebohai(datestr:String):Int = {
      val month: Int = datestr.substring(5,7).toInt
      println("month:" + month)
      val day:Int = datestr.substring(8,10).toInt
      println("day:" + day)
      12+1-month+(if ((1-day) > 0) 1 else 0)
  }


  def parse2Date(date: String,date_type:String): Date = {
    val dateformat = FastDateFormat.getInstance(date_type)
    dateformat.parse(date)
  }

  def addMonth(date: Date, numMonth: Integer): Date = {
    val cal = Calendar.getInstance();
    cal.setTime(date)
    cal.add(Calendar.MONTH, numMonth)
    cal.getTime
  }

  def addMonth2Timestamp(date: String,date_type:String, numMonth: Integer):Timestamp = {
    val dateformat = FastDateFormat.getInstance(date_type)
    dateformat.parse(date)
    val cal = Calendar.getInstance();
    cal.setTime(dateformat.parse(date))
    cal.add(Calendar.MONTH, numMonth)
    new Timestamp(cal.getTimeInMillis)
  }

}
