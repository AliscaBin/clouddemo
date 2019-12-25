package com.wangbin.clouddemo.util

import java.sql.Timestamp

import com.wangbin.clouddemo.util.ToNum.parse2Date
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_timestamp

object Testto_timestamp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._

    val dataframe = spark.read.option("header", true)
      .option("inferSchema",false)
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("sep", "|")
      .csv("D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\test.csv")
    dataframe.printSchema()
    dataframe.filter(to_timestamp($"date","yyyyMMdd") > new Timestamp(parse2Date("2019-02-28","yyyy-MM-dd").getTime)).show()

  }
}
