package com.wangbin.clouddemo.util

import java.sql.Timestamp
import java.util.Date

import com.wangbin.clouddemo.util.ToNum.parse2Date
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

case class order(id:Int,name:String,price:Double,nums:Int)
object TestRowNumber {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._
    val df: DataFrame = Seq(
      order(1, "张䶮", 12.12, 3),
      order(1, "李䶮飞", 45.2, 3),
      order(1, "张明䶮", 56.2, 1),
      order(2, "hongbo", 45.12, 6),
      order(2, "hongbo", 33.2, 3),
      order(2, "hongbo", 45.23, 6),
      order(2, "hongbo", 21.4, 2)
    ).toDF()
    val schema = df.schema.fieldNames
//    df.repartition(1).select(concat_ws("|",schema.map(df(_)):_*) as schema.mkString("|")).write.option("charset", "iso-8859-1").option("sep", "|").mode("overwrite").text("D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\df.txt")



    df.repartition(1)
      .select(concat_ws("|",schema.map(df(_)):_*) as schema.mkString("|"))
//      .write.option("charset", "iso-8859-1")
//      .write.option("charset", "iso-8859-1")
      .write.option("charset", "GBK")
      .option("sep", "|")
      .mode("overwrite")
      .text("D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\df.txt")
//    val df2 = df.withColumn("row_seq",row_number().over(Window.partitionBy($"name").orderBy($"price".desc)))
//      .filter($"row_seq"===1)
//      .show()
//
//
//    df2.select("id","name","price").show()


//    val dataframe = spark.read.option("header", true)
//      .option("inferSchema",false)
//      .option("ignoreLeadingWhiteSpace", true)
//      .option("ignoreTrailingWhiteSpace", true)
//      .option("sep", "|")
//      .csv("D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\test.csv")
//    dataframe.printSchema()
//    dataframe.filter(to_timestamp($"date","yyyy/MM/dd") > new Timestamp(parse2Date("2019-02-28","yyyy-MM-dd").getTime)).show()



    //    dataframe.select("id","name").show()
  }
}
