package com.wangbin.clouddemo.util



import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}

object writeOracle {

  val spark: SparkSession = SparkBuilder.builder()
  import spark.implicits._
  val person1: DataFrame = Seq(Person(1, "zhangsan", "zhangbj"),
    Person(2, "lisi", "libj"),
    Person(3, "wangwu", "wangsh"),
    Person(4, "zhaoliu", "zhaotj"),
    Person(5, "tianqi", "tiansh")
  ).toDF()


  val url = "jdbc:oracle:thin:@//ip:1521/uprr"
  val user = "datacore"
  val password = "datacore"
  val driver = "oracle.jdbc.driver.OracleDriver"

  val dbmap = Map("url" -> url, "user" -> user, "password" -> password, "driver" -> driver)


  val properties = new Properties()
  properties.setProperty("url", url)
  properties.setProperty("user", user)
  properties.setProperty("password", password)
  properties.setProperty("driver", driver)

  def saveSummary(df: DataFrame, sqlContext: SQLContext): Unit = {
    //DateTypes.createStructField()
    val schema= StructType(List(StructField("mon_th",IntegerType, true)
      , StructField("current_date", DateType, true)
      , StructField("amount", DoubleType, true)
      , StructField("account_code", StringType, true)
      , StructField("summary_code", StringType, true)
      , StructField("project", StringType, true)
    ))
//    val df: DataFrame = sqlContext.createDataFrame(rowrdd, schema)
    df.write.mode(SaveMode.Append).jdbc(url,"表名",properties)
  }

}
