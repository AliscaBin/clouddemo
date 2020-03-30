package com.wangbin.clouddemo.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object TestSparkDate {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._
    val path:String = "D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\test.csv"
    val person = spark.read
        .format("csv")
      .option("header", "true")
//      .option("inferSchema", "true")
      .option("sep","|")
      .load(path)

    //获取当月第一天日期
//    person.select(
//      concat_ws("-",substring($"date",1,7),lit("01")) as "date",
//      concat(substring($"date",1,7),lit("-01")) as "date1",
//      trunc($"date","MM") as "date2"
//    ).show()

    def toInts(s: String): Option[Int] = {
      try {
        Some(s.toInt)
      } catch {
        case e: Exception =>Some(0)
      }
    }

    val toDataUdf = udf((date:String,format:String,num: String) =>{
  val first = DateUtils.getTimestampByDate(DateUtils.parse2Date(date,format)).getTime.toDouble
  val second = System.currentTimeMillis().toDouble

  (first - second-toInts(num).get)
})
    person.withColumn("rownum",row_number().over(Window.orderBy($"id")))
//      .show()
//      .withColumn("current_date",(to_timestamp(toDataUdf($"date",lit("yyyy-MM-dd")),"yyyy-MM-dd"))).show()
      .withColumn("current_date",toDataUdf($"date",lit("yyyy-MM-dd"),$"rownum")).show()
//      .select(when($"address".isNotNull,$"address").otherwise("默认")).show()

    //获取当前系统时间
    val temp = person.select(date_add(to_date($"date"),-1) as "date").filter($"date" === "2020-03-27").cache()
    temp.show()
  System.out.println(temp.first().apply(0))

    person.withColumn("current",lit("2019-07-29")).select(trunc($"date","yyyy") as "datefirst",
    $"date" as "date", $"current" as "current", $"current" >= trunc($"date","yyyy") as "flag",trunc(lit("2019-07-29 20:12:10"),"yyyy") as "2019firstday").show()

  }
}
