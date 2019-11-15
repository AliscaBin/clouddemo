package com.wangbin.clouddemo

import java.sql.Timestamp
import java.util.Date

import com.wangbin.clouddemo.util.SparkBuilder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

object Test extends App {

  val spark = SparkSession
    .builder()
    //      .appName("Postal Fund App")
    .master("local[2]")
    .getOrCreate()

  val sdf = FastDateFormat.getInstance("yyyyMMdd")
  def parse(date: String): Date = {
    sdf.parse(date)
  }

  def getTimestampByDate(date: Date): Timestamp = {
    new Timestamp(date.getTime)
  }
  private val date: Date = parse("20190813")
  println(date)
  private val timestamp: Timestamp = getTimestampByDate(date)
  println(timestamp)

  import spark.implicits._
  spark.read.format("csv")
    .schema(Encoders.product[O_CR_IND_INFO].schema)
    .option("header", "false")
    .option("inferSchema", "true")
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .option("nullValue", "0")
    .option("sep", "|")
    .load("D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\o_cr_ind_info_20190813.txt")
    .filter(to_timestamp($"HIS_BEGIN_DATE") <= timestamp
      and to_timestamp($"HIS_END_DATE") > timestamp)
    .show()


}
