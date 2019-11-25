package com.wangbin.clouddemo.util

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

case class StudentErr(id:String,name:String,address:String,date:String)

object TestCheckSchema {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._
    val path:String = "D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\test.csv"
    val stuDataSet: Dataset[_] = SparkBuilder.getDataFrame(spark,Encoders.product[StudentErr].schema,path,true)
//    println("ErrorFiles:"+SparkBuilder.ExceptionFiles.mkString(","))
    stuDataSet.show()



  }



}
