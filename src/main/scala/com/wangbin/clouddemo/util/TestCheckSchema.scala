package com.wangbin.clouddemo.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

case class StudentErr(id:String,name:String,address:String,date:String,time:String,hobby:String)

object TestCheckSchema {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._

//    spark.createDataFrame(spark.sparkContext.emptyRDD,Encoders.product[StudentErr].schema)
    
//    spark.createDataFrame(spark.sparkContext.emptyRDD[Row],Encoders.product[StudentErr].schema).as[StudentErr]
    
    
    val path:String = "D:\\IdeaProjects\\clouddemo\\src\\main\\resources\\test.csv"

//    val stuDataSet: DataFrame = spark.read.option("sep","|").option("inferSchema", "true").schema(Encoders.product[StudentErr].schema).csv(path)
//    val stuDataSet: Dataset[_] = SparkBuilder.getDataFrame(spark,Encoders.product[StudentErr].schema,path,false)
//    val stuDataSet: Dataset[_] = spark.read.option("sch")
//    println("ErrorFiles:"+SparkBuilder.ExceptionFiles.mkString(","))
//    stuDataSet.show()
    def makeRow(data:String,schema:StructType):Row={
      schema.fieldNames.length
      val fields: Array[String] = data.split("|")
      print(fields.toString)
      Row(fields:_*)
    }
    val studentErr: RDD[Row] = spark.sparkContext.textFile(path).map(x=>Row(x.split("\\|"):_*))

    val df: DataFrame = spark.createDataFrame(studentErr,Encoders.product[StudentErr].schema)
//    val studentErr: RDD[String] = spark.sparkContext.textFile(path)
//      .toDF(Encoders.product[StudentErr].schema.fieldNames.mkString(","))

    println("-----")
    df.show()
    println("-----")

//    df.groupBy("name").agg(max("time")).show()
    df.distinct().show()
//    studentErr.collect().foreach(println(_))

  }



}
