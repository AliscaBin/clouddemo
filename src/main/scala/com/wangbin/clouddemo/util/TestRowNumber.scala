package com.wangbin.clouddemo.util

import java.sql.Timestamp
import java.util.Date

import com.wangbin.clouddemo.util.ToNum.parse2Date
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

case class order(id: Int, name: String, price: Double, nums: Int)

object TestRowNumber {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._
    val df: DataFrame = Seq(
      order(1, "张䶮", 12.12, 3),
      order(2, "李䶮飞", 45.2, 3),
      order(5, "李䶮飞飞", 45.2, 3),
      order(3, "张李明䶮", 56.2, 1),
      order(6, "hongbo李", 45.12, 6),
      order(7, "hongbo", 33.2, 3),
      order(4, "hong李bo", 45.23, 6)
    ).toDF()
    df.createOrReplaceTempView("order")
    //    spark.sql("select id,name,price,nums,cast(price*nums as decimal(18,4)) as sum from order").show()  //小数精度


    //保留小数位数
    df.selectExpr("id",
      "name",
      "price",
      "nums",
      "cast(price*nums as decimal(18,4)) as sum"
    ).show()



//    df = df.selectExpr("round(money,2) as money" ,"created_ts","updated_ts");
//    df = df.selectExpr("cast(money as decimal(20,2)) as money" ,"created_ts","updated_ts");


    //    df.sort("id").show()


    val schema = df.schema.fieldNames
    //    df.repartition(1).select(concat_ws("|",schema.map(df(_)):_*) as schema.mkString("|")).write.option("charset", "iso-8859-1").option("sep", "|").mode("overwrite").text("D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\df.txt")
    println(schema.mkString("|"))
    //    df.filter(! $"name".like("李%")).show()

    //println(schema)
    //    df.repartition(1)
    //      .select(concat_ws("|",schema.map(df(_)):_*) as schema.mkString("|"))
    ////      .write.option("charset", "iso-8859-1")
    ////      .write.option("charset", "iso-8859-1")
    //      .write.option("charset", "GBK")
    //      .option("sep", "|")
    //      .mode("overwrite")
    //      .text("D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\df.txt")
    //    val df2 = df.withColumn("row_seq",row_number().over(Window.partitionBy($"name").orderBy($"price".desc)))
    //      .filter($"row_seq"===1)
    //      .show()
    //
    //
    //    df2.select("id","name","price").show()


//        val dataframe = spark.read.option("header", true)
//          .option("inferSchema",false)
//          .option("ignoreLeadingWhiteSpace", true)
//          .option("ignoreTrailingWhiteSpace", true)
//          .option("sep", "|")
//          .csv("D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\test.csv")
//        dataframe.printSchema()
//        dataframe.filter(to_timestamp($"date","yyyyMMdd") > new Timestamp(parse2Date("2019-02-28","yyyy-MM-dd").getTime)).show()


    //    dataframe.select("id","name").show()
  }
}
