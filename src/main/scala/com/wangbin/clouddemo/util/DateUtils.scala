package com.wangbin.clouddemo.util

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

case class DateUtils()

object DateUtils {

  val sdf = FastDateFormat.getInstance("yyyyMMdd")
  val smf = FastDateFormat.getInstance("yyyyMM")
  val syf = FastDateFormat.getInstance("yyyy")

  val MS_PERDAY = 1000 * 60 * 60 * 24

  /**
   * 获取输入天的前一天时间
   *
   * @param date 输入天
   * @return
   */
  def getBeforeDay(date: Date): Date = {
    val beginDate = Calendar.getInstance();
    beginDate.setTime(date)
    beginDate.add(Calendar.DATE, -1)
    beginDate.getTime
  }

  /**
   * 获取输入天的后一天时间
   *
   * @param date 输入天
   * @return
   */
  def getAfterDay(date: Date): Date = {
    val beginDate = Calendar.getInstance();
    beginDate.setTime(date)
    beginDate.add(Calendar.DATE, 1)
    beginDate.getTime
  }

  /**
   * 获取输入天的前一天时间
   *
   * @param date 输入天
   * @return
   */
  def getTheSameDayOfLastMonth(date: Date): Date = {
    val beginDate = Calendar.getInstance();
    beginDate.setTime(date)
    beginDate.add(Calendar.MONTH, -1)
    beginDate.getTime
  }

  /** *
   * 结束日期与起始日期的时间差（以天为单位）<br>
   *
   * @example getStartToEndDiffer("20180101","20180401")
   * @param start 起始日期
   * @param end   结束日期
   * @return 时间差
   */
  def getStartToEndDiffer(start: Date, end: Date): Int = {
    ((end.getTime - start.getTime) / MS_PERDAY).toInt
  }

  /** *
   *
   * @example add("20180101",90),add("20180101",-90)
   * @param start  起始日期
   * @param dayNum 天数
   * @return
   */
  def add(start: Date, dayNum: Int): Date = {
    val beginDate = Calendar.getInstance();
    beginDate.setTime(start)
    beginDate.add(Calendar.DAY_OF_MONTH, dayNum)
    beginDate.getTime
  }

  /** *
   * 返回2个日期之间日期集合(包括起始，结束日期)
   * 起始日期要小于结束日期
   *
   * @param start
   * @param end
   * @return
   */
  def getEveryDayByDate(start: Date, end: Date): Array[Date] = {
    val differDay = getStartToEndDiffer(start, end) + 1
    val arrayDay = new Array[Date](differDay)
    for (i <- 0 until differDay) {
      arrayDay(i) = add(start, i)
    }
    arrayDay
  }

  /** *
   * 返回指定年月份的最后一天
   *
   * @param date 格式yyyyMMdd
   */
  def getLastDateOnMonth(date: Date): Date = {
    val cal = Calendar.getInstance();
    cal.setTime(date)
    cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
    cal.getTime
  }

  /**
   * 返回指定年份的最后一天
   *
   * @param date
   * @return
   */
  def getLastDateOnYear(date: Date): Date = {
    val cal = Calendar.getInstance();
    cal.setTime(date)
    cal.set(Calendar.DAY_OF_YEAR, cal.getActualMaximum(Calendar.DAY_OF_YEAR));
    cal.getTime
  }

  /** *
   * 获取指定月份的天数
   *
   * @param date
   * @return
   */
  def getDayNumOnMonth(date: Date): Int = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    var day = cal.getMaximum(Calendar.DAY_OF_MONTH)
    day
  }

  /** *
   *
   * @param date
   * @return
   */
  def getFirstDateOnMonth(date: Date): Date = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    calendar.getTime
  }

  def getFirstDateOnYear(date: Date): Date = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.set(Calendar.DAY_OF_YEAR, 1)
    calendar.getTime
  }

  /**
   * 判断输入日期是否为所年月的第一天
   *
   * @param date 输入日期
   * @return 判断结果（Boolena）
   */
  def isFirstDateOnMonth(date: Date): Boolean = {
    val calFirstDate = Calendar.getInstance()
    calFirstDate.setTime(date)
    calFirstDate.set(Calendar.DAY_OF_MONTH, 1)
    calFirstDate.getTime

    val calDate = Calendar.getInstance()
    calDate.setTime(date)

    calFirstDate.get(Calendar.YEAR) == calDate.get(Calendar.YEAR) &&
      calFirstDate.get(Calendar.MONTH) == calDate.get(Calendar.MONTH) &&
      calFirstDate.get(Calendar.DAY_OF_MONTH) == calDate.get(Calendar.DAY_OF_MONTH)

  }

  def isLastDateOnMonth(date: Date): Boolean = {
    val cal = Calendar.getInstance();
    cal.setTime(date)
    cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
    cal.getTime

    val calDate = Calendar.getInstance()
    calDate.setTime(date)

    cal.get(Calendar.YEAR) == calDate.get(Calendar.YEAR) &&
      cal.get(Calendar.MONTH) == calDate.get(Calendar.MONTH) &&
      cal.get(Calendar.DAY_OF_MONTH) == calDate.get(Calendar.DAY_OF_MONTH)

  }

  /**
   * 判断当前日期是否为所在年第一天
   *
   * @param date 输入日期（被判断日期）
   * @return 判断结果（Boolena）
   */
  def isFirstDateOnYear(date: Date): Boolean = {

    val calFirstDate = Calendar.getInstance()
    calFirstDate.setTime(date)
    calFirstDate.set(Calendar.DAY_OF_YEAR, 1)
    calFirstDate.getTime

    val calDate = Calendar.getInstance()
    calDate.setTime(date)

    calFirstDate.get(Calendar.YEAR) == calDate.get(Calendar.YEAR) &&
      calFirstDate.get(Calendar.MONTH) == calDate.get(Calendar.MONTH) &&
      calFirstDate.get(Calendar.DAY_OF_MONTH) == calDate.get(Calendar.DAY_OF_MONTH)
  }

  def isLastDateOnYear(date: Date): Boolean = {
    val cal = Calendar.getInstance();
    cal.setTime(date)
    cal.set(Calendar.DAY_OF_YEAR, cal.getActualMaximum(Calendar.DAY_OF_YEAR));
    cal.getTime

    val calDate = Calendar.getInstance()
    calDate.setTime(date)

    cal.get(Calendar.YEAR) == calDate.get(Calendar.YEAR) &&
      cal.get(Calendar.MONTH) == calDate.get(Calendar.MONTH) &&
      cal.get(Calendar.DAY_OF_MONTH) == calDate.get(Calendar.DAY_OF_MONTH)
  }

  /** *
   * 获取指定日期的上一个月 比如yyyyMMdd
   * <br>20181002 返回201809 ,20180120 返回201712
   *
   * @param date yyyyMMdd
   * @return yyyyMM
   */
  def getPreviousMonth(date: Date): Date = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    // cal.set(Calendar.DAY_OF_MONTH,1)
    cal.add(Calendar.MONTH, -1)
    cal.getTime
  }


  /** *
   * 获取指定日期 是星期几
   *
   * @param date
   * @return
   */
  def getDayOfWeekZeroIndex(date: Date): Int = {
    val cal = Calendar.getInstance();
    cal.setTime(date)
    cal.get(Calendar.DAY_OF_WEEK) - 1
  }

  /**
   * 判断输入天是否是月末最后一天
   *
   * @param date 输入天 ,格式"YYYYMMDD"
   * @return
   */
  def isTheEndOfAMonth(date: String): Boolean = {
    val cal = Calendar.getInstance
    cal.set(date.substring(0, 4).toInt, 2, 0)
    val isLeapYear = cal.get(Calendar.DAY_OF_MONTH) match {
      case 28 => false
      case 29 => true
    }

    val isTheEnd = date.substring(4, 8) match {
      case "0131" => true
      case "0228" => !isLeapYear & true
      case "0229" => true
      case "0331" => true
      case "0430" => true
      case "0531" => true
      case "0630" => true
      case "0731" => true
      case "0831" => true
      case "0930" => true
      case "1031" => true
      case "1130" => true
      case "1231" => true
      case _ => false
    }

    isTheEnd
  }

  /** *
   * 对时间进行月份加减
   *
   * @param date
   * @param numMonth
   * @return
   */
  def addMonth(date: Date, numMonth: Integer): Date = {
    val cal = Calendar.getInstance();
    cal.setTime(date)
    cal.add(Calendar.MONTH, numMonth)
    cal.getTime
  }

  /** *
   * 通用日期加减，年月日 都可以进行加减
   *
   * @param date
   * @param format    日期格式
   * @param num
   * @param dateLevel 与Calendar 的日期类型的常量值对应
   */
  def addDate(date: Date, format: String, num: Integer, dateLevel: Int): Date = {
    val sdff = new SimpleDateFormat(format)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(dateLevel, num)
    cal.getTime
  }

  def getCurrentTime(): String = {

    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    date
  }

  /**
   * 日期转yyyyMMdd 格式字符串
   *
   * @param date
   * @return
   */
  def format(date: Date): String = {
    sdf.format(date)
  }

  /**
   * 日期转yyyyMM 格式字符串
   *
   * @param date
   * @return
   */
  def formatMon(date: Date): String = {
    smf.format(date)
  }

  /**
   * 日期转yyyy 格式字符串
   *
   * @param date
   * @return
   */
  def formatYear(date: Date): String = {
    syf.format(date)
  }

  /**
   *
   * @param date
   * @param toFormat
   * @return
   */
  def format(date: Date, toFormat: String): String = {
    new SimpleDateFormat(toFormat).format(date)
  }

  /**
   * yyyyMMdd 转Date
   *
   * @param date
   * @return
   */
  def parse(date: String): Date = {
    sdf.parse(date)
  }

  def parse2Date(date: String,date_type:String="yyyyMMdd"): Date = {
    val dateformat = FastDateFormat.getInstance(date_type)
    dateformat.parse(date)
  }

  def date2String(date: Date,date_type:String): String = {
    val dateformat = FastDateFormat.getInstance(date_type)
    dateformat.format(date)
  }

  /**
   * 对日期添加月份后返回timestamp
   * @param date
   * @param date_type
   * @param numMonth
   * @return
   */
  def addMonth2Timestamp(date: String,date_type:String, numMonth: Integer):Timestamp = {
    val dateformat = FastDateFormat.getInstance(date_type)
    dateformat.parse(date)
    val cal = Calendar.getInstance();
    cal.setTime(dateformat.parse(date))
    cal.add(Calendar.MONTH, numMonth)
    new Timestamp(cal.getTimeInMillis)
  }

  /**
   * 渤海银行业务时间计算需要
   * @param date1
   * @param date2
   * @return
   */
  def rangeYMD(date1: Date, date2: Date): String = {
    val cal1: Calendar = Calendar.getInstance()
    val cal2: Calendar = Calendar.getInstance()
    cal1.setTime(date1)
    cal2.setTime(date2)
    val rangeY: Int = cal1.get(Calendar.YEAR) - cal2.get(Calendar.YEAR)
    val rangeM: Int = cal1.get(Calendar.MONTH) - cal2.get(Calendar.MONTH)
    val rangeD: Int = cal1.get(Calendar.DAY_OF_MONTH) - cal2.get(Calendar.DAY_OF_MONTH)
    val flag = if (rangeD >= 1) 1 else 0
    (rangeY*12+rangeM+flag).toString
  }

  /**
   * yyyyMM 转Date
   *
   * @param date
   * @return
   */
  def parseMon(date: String): Date = {
    smf.parse(date)
  }

  /**
   * yyyy 转Date
   *
   * @param date
   * @return
   */
  def parseYear(date: String): Date = {
    syf.parse(date)
  }

  def getTimestampByDate(date: Date): Timestamp = {
    new Timestamp(date.getTime)
  }

  /**
   * 判断是否是季度末，结合月底最后一天里调用该方法可判断是否是季度末最后一天
   *
   * @param date 天日期
   * @return
   */
  def isEndOfQuarter(date: Date): Boolean = {
    val dateStr = format(date)
    if (isTheEndOfAMonth(dateStr)) {
      val month = dateStr.substring(4, 6).toInt
      month % 3 == 0
    } else {
      false
    }
  }

  /**
   * 是否是周四
   *
   * @param date 天日期
   * @return
   */
  def isThuOfWeek(date: Date): Boolean = {
    getDayOfWeekZeroIndex(date) == 4
  }

  /**
   * 判断是否是周五
   *
   * @param date
   * @return
   */
  def isFriOfWeek(date: Date): Boolean = {
    getDayOfWeekZeroIndex(date) == 5
  }

  /**
   * 是否是周一
   *
   * @param date 天日期
   * @return
   */
  def isMonOfWeek(date: Date): Boolean = {
    getDayOfWeekZeroIndex(date) == 1
  }

  def isEndOfWeek(date: Date): Boolean = {
    getDayOfWeekZeroIndex(date) == 0
  }

  def addYear(date:Date,numYear:Integer): Date ={
    val cal = Calendar.getInstance();
    cal.setTime(date)
    cal.add(Calendar.YEAR,numYear)
    cal.getTime
  }

  def main(args: Array[String]): Unit = {
    val yesterday = parse("20191231")
//    println(isLastDateOnMonth(yesterday))
//    println(isLastDateOnYear( yesterday))
    val lastday: String = date2String(getLastDateOnMonth(parse2Date("20190812","yyyyMMdd")),"yyyyMMdd")
    println("lastday:" + lastday)

    val i: Int = getDayNumOnMonth(parse2Date("20190812","yyyyMMdd"))
    println("nums day : " + i)
    //    println(getPreviousMonth("20181202"))
    //    println(isTheEndOfAMonth("20000228"))
    //    println(getFirstDateOnMonth(sdf.parse("20171120")))
    //    println(getStartToEndDiffer(DateUtil.parse("20171120"), DateUtil.parse("20171123")))
    //    println(DateUtil.getEveryDayByDate(DateUtil.parse("20171120"), DateUtil.parse("20171123")).length)
    //    // println(add(DateUtil.parse("20171120"),1))
    //
    //    println(sdf.format(new Date(1530547200 * 1000L)))
    //    println(sdf.format(new Date(20181120)))
    //
    //    println(DateUtil.format(getLastDateOnYear(new Date())))
    //    println(DateUtil.format(getFirstDateOnYear(new Date())))
    //    println(formatMon(addMonth(parse("20171231"), -3)))
    //    println(parseMon("201711").before(parseMon("201711")))
    //    println(format(DateUtil.add(parse("20171021"), -10)))
    //    println(getBeforeDay(parse("20171021")))
    //    println(getBeforeDay(parse("20170101")))
    //
    //    println(getTheSameDayOfLastMonth(parse("20170101")))
    //    println(getTheSameDayOfLastMonth(parse("20170330")))
    //
    //    println(isFirstDateOnYear(parse("20170101")))
    //    println(isFirstDateOnYear(parse("20170102")))
    //    println(isFirstDateOnMonth(parse("20170401")))
    //    println(isFirstDateOnMonth(parse("20170402")))
  }

}
