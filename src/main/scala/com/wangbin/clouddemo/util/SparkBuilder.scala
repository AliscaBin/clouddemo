package com.wangbin.clouddemo.util

import java.io.File
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

object SparkBuilder {

  private var spark:SparkSession = null

  private val separator = '|'

  var ExceptionFiles = List()

  def builder(flag:Boolean=false): SparkSession ={
    if (spark == null){
      synchronized{
        val sparkConf = new SparkConf()
        if (flag){
          sparkConf.setMaster("local[5]")
          sparkConf.setAppName("spark_handle_data_test ")
        }
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("log.level","error")
        spark = SparkSession
          .builder()
          .config(sparkConf)
//          .enableHiveSupport()
          .getOrCreate()
      }
    }
    spark
  }


  def getDataFrame(spark: SparkSession, schema: StructType,path:String):Dataset[_]={

    val textRdd: RDD[String] = spark.sparkContext.textFile(path)
    val errorRdd: RDD[String] = textRdd.filter(_.split(separator).size > schema.size)
    if (errorRdd.isEmpty()){
      val RowRdd: RDD[Row] = textRdd.map(_.split(separator)).map(lines => Row(lines:_*))
      spark.createDataFrame(RowRdd,schema)
    }else{
      val name: String = new File(path).getName.split('.')(0)
      ExceptionFiles ++ name
      null
    }

//    null
  }
}
