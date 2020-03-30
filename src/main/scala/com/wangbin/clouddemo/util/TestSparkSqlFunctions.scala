package com.wangbin.clouddemo.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TestSparkSqlFunctions {
  def main(args: Array[String]): Unit = {
    val path: String = "D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\test.csv"
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._
    val person: DataFrame = spark.read
      //        .schema("null")
      .option("header", "true")
      //      .option("inferSchema", "true")
      .option("sep","|")
      .csv(path)
    person.show(false)
//    person.select(to_date($"date","YYYYMMDD") as "date").show()

    person.select("date").show()
//    +--------+
//    |    date|
//    +--------+
//    |20190712|
//    |20180328|
//    |20180328|
//    |20180328|
//    |20180328|
//    |20180328|
//    |20190228|
//    +--------+
    val datedf: DataFrame = person.select(to_date($"date","yyyymmdd") as "date_result")
    SparkBuilder.saveDF2TXT(spark,datedf,"datetext.txt")
  }

}
