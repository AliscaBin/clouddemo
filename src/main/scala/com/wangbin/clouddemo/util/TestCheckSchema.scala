package com.wangbin.clouddemo.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Student(id:String,name:String,address:String,date:String)

object TestCheckSchema {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._

    SparkBuilder.getDataFrame(spark,)
  }



}
