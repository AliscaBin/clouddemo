package com.wangbin.clouddemo.util


import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
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
    val schema = StructType(List(StructField("mon_th", IntegerType, true)
      , StructField("current_date", DateType, true)
      , StructField("amount", DoubleType, true)
      , StructField("account_code", StringType, true)
      , StructField("summary_code", StringType, true)
      , StructField("project", StringType, true)
    ))
    //    val df: DataFrame = sqlContext.createDataFrame(rowrdd, schema)
    df.write.mode(SaveMode.Append).jdbc(url, "表名", properties)
  }

  /**
   * 连接数据库工具类
   */

    /**
     * 读取Oracle数据库
     *
     * @param spark
     * @param dbtable
     * @return
     */
    def readPMSGSOracleData(spark: SparkSession, dbtable: String) = {
      val data: DataFrame = spark.read.format("jdbc")
        .option("url", "jdbc:oracle:thin:@//10.212.242.XXX:11521/pmsgs")
        .option("dbtable", dbtable)
        .option("user", "qyw")
        .option("password", "XXX")
        .load()
      data
    }

    /**
     * 读取mysql数据库
     *
     * @param spark
     * @param dbtable
     * @return
     */
    def readUSAPPData(spark: SparkSession, dbtable: String) = {
      val data: DataFrame = spark.read.format("jdbc")
        .option("url", "jdbc:mysql://21.76.120.XX:3306/us_app")
        .option("dbtable", dbtable)
        .option("user", "root")
        .option("password", "XXX")
        .load()
      data
    }

    /**
     * 将数据写入到Mysql数据库(追加写入)
     *
     * @param result
     * @param dbtable
     */
    def writeData(result: DataFrame, dbtable: String): Unit = {
      result.write.mode(SaveMode.Append).format("jdbc")
        .option(JDBCOptions.JDBC_URL, "jdbc:mysql://21.76.120.XX:3306/us_app?rewriteBatchedStatement=true")
        .option("user", "root")
        .option("password", "XXX")
        .option(JDBCOptions.JDBC_TABLE_NAME, dbtable)
        .option(JDBCOptions.JDBC_TXN_ISOLATION_LEVEL, "NONE") //不开启事务
        .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 500) //设置批量插入
        .save()
    }
  }


