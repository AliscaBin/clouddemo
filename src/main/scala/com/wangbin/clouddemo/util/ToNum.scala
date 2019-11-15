package com.wangbin.clouddemo.util

import java.io.File
import java.sql.Timestamp
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object ToNum extends App{
  val num = ".3"
  println(num.toDouble.formatted("%.3f").toString)

  def parse2Date(date: String,date_type:String): Date = {
    val dateformat = FastDateFormat.getInstance(date_type)
    dateformat.parse(date)
  }

  println(new Timestamp(parse2Date("2019-02-28","yyyy-MM-dd").getTime))

  println(new File("D:/haha/hah.txt").getName.split('.')(0))

//  println((num * null) < 10)
}
