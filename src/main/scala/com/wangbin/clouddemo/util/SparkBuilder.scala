package com.wangbin.clouddemo.util

import java.io.{File, FileWriter}
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

object SparkBuilder {

  private var spark: SparkSession = null

  private val separator = '|'

  def builder(flag: Boolean = false): SparkSession = {
    if (spark == null) {
      synchronized {
        val sparkConf = new SparkConf()
        if (flag) {
          sparkConf.setMaster("local[*]")
          sparkConf.setAppName("spark_handle_data_test ")
        }
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.sql.orc.impl", "native")
        sparkConf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        sparkConf.set("log.level", "error")
        spark = SparkSession
          .builder()
          .config(sparkConf)
          //          .enableHiveSupport()
          .getOrCreate()
      }
    }
    spark
  }


  def getDataFrame(spark: SparkSession, schema: StructType, path: String, header: Boolean = false): Dataset[_] = {

    val textRdd: RDD[String] = spark.sparkContext.textFile(path)
    val errorRdd: RDD[String] = textRdd.filter(_.split(separator).length > schema.size)
    if (errorRdd.isEmpty()) {
      var RowRdd:RDD[Row] = null
      if (header) {
        val head = textRdd.take(1)
        RowRdd = textRdd.filter(!head.contains(_)).map(_.split(separator)).map(lines => Row(lines: _*))
      }else{
        RowRdd = textRdd.map(_.split(separator)).map(lines => Row(lines: _*))
      }
      spark.createDataFrame(RowRdd, schema)
    } else {
      val errLines: Array[String] = errorRdd.collect()
      val name: String = new File(path).getName.split('.')(0)
      val writer = new FileWriter("Exception_Table_"+name)
      writer.write(errLines.mkString(System.lineSeparator()))
      writer.close()
      throw new Exception(s"Found Exception Table: ${name}")
      spark.emptyDataFrame
    }

    //    null
  }
}
