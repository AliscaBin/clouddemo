package com.wangbin.clouddemo.util

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._

object Testsubstring {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._
    println("create the DataFrame")

    val person: DataFrame = Seq(Person(1, "zhangsan", "zhangbj"),
      Person(2, "lisi", "libj"),
      Person(3, "wangwu", "wangsh"),
      Person(4, "zhaoliu", "zhaotj"),
      Person(5, "tianqi", "tiansh")
    ).toDF()

    person.select(substring($"name",1,3) as "name").show()
//    val person: DataFrame = Seq(Person(1, "2019-08-21 13:12:33", "zhangbj"),
//      Person(2, "lisi", "libj"),
//      Person(3, "wangwu", "wangsh"),
//      Person(4, "zhaoliu", "zhaotj"),
//      Person(5, "tianqi", "tiansh")
//    ).toDF()

//    val person1: DataFrame = person.withColumn("update",lit("20191210"))
//    val person2: DataFrame = person.withColumn("update",lit("20191211"))

//    person1.write.mode("overwrite").orc("/person1/date=20191210")
//    person2.write.mode("overwrite").orc("/person1/date=20191211")

//    spark.read.orc("/person").printSchema()

//    person1.unionByName(person2).write.partitionBy("update").mode("overwrite").orc("/person")

//    spark.read.orc("D:\\person\\update=20191210").printSchema()

    /*设置basePath使得分区字段有效*/
//    spark.read.option("basePath","/person").orc("/person/date=20191211").printSchema()


//    person.select(substring($"name",1,10) as  "name").show()


    //df 为空 按分区保存
//    val df1: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],Encoders.product[Person].schema)
//    df1.withColumn("update",lit("20191210")).write.mode("overwrite").save("/person/update=20191210")
//    df1.withColumn("update",lit("20191210")).printSchema()

try{
  val frame: DataFrame = spark.read.option("basePath","/person").orc("/person/update=20191210")
}catch {
  case ec:Exception=>
    val frame: DataFrame =spark.sparkContext.parallelize(Array("nodata")).toDF()
}

/*    if (frame.rdd.isEmpty()){
      println("no")
    }*/
//    frame.printSchema()
//    val frame2: DataFrame = spark.read.orc("/person/update=20191211")
//    println("test")
//    frame2.show()


//    val df1: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],Encoders.product[Person].schema)
//   val r= df1.withColumn("update",lit("20191210"))
     //.write.format("csv").mode("overwrite").save("/person/update=20191210/")
//    r.rdd.saveAsTextFile("/person/update=20191210/")




  }
}
