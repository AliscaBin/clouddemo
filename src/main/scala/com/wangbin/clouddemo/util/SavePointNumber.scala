package com.wangbin.clouddemo.util

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object SavePointNumber {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._
    val df: DataFrame = Seq(
      order(1, "张䶮", 12.12, 3),
      order(1, "李䶮飞", 45.2, 3),
      order(2, "李䶮飞飞", 56.2, 3),
      order(2, "张李明䶮", 56.2, 1),
      order(3, "hongbo李", 45.12, 6),
      order(4, "hong李bo", 33.2, 6),
      order(4, "hongbo", 33.2, 6)
    ).toDF()


    df.select($"id" as "ID",$"name" as "NAME",row_number().over(Window.partitionBy($"id").orderBy($"price".desc,$"nums".desc)) as "Rank").show()


  }

}
