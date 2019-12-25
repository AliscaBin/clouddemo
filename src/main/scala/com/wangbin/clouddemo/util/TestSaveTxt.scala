package com.wangbin.clouddemo.util

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object TestSaveTxt {
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
    val value: Dataset[order] = df.select($"id" as "id",$"price" as "price",$"nums" as "nums",$"name" as "name").as[order]
    value.show()


    val schema = df.schema.fieldNames
//    df.repartition(1).select(concat_ws("|",schema.map(df(_)):_*) as schema.mkString("|")).write.option("charset", "iso-8859-1").option("sep", "|").mode("overwrite").text("D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\df.txt")
    df.repartition(1).select(concat_ws("|",schema.map(df(_)):_*) as schema.mkString("|")).write.option("sep", "|").mode("overwrite").text("D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\df.txt")

  }
}
