package com.wangbin.clouddemo.util

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._

case class Person(id: Int, name: String, address: String)

case class Student(id: Int, name: String, address: String, high: Int)

case class Student1(id: Int, address: String, name: String, high: Int)


object testunion {
  def main(args: Array[String]): Unit = {
    println("get the SparkSession")
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._
    println("create the DataFrame")
//    val person: DataFrame = Seq(Person(1, "zhangsan", "zhangbj"),
//      Person(2, "lisi", "libj"),
//      Person(3, "wangwu", "wangsh"),
//      Person(4, "zhaoliu", "zhaotj"),
//      Person(5, "tianqi", "tiansh")
//    ).toDF()
//
//    val student: DataFrame = Seq(Student(1, "zhangsan", "zhangbj", 11),
//      Student(2, "lisi", "libj", 12),
//      Student(3, "wangwu", "wangsh", 13),
//      Student(4, "zhaoliu", "zhaotj", 14),
//      Student(6, "tianqi", "tiansh", 15)
//    ).toDF()
//
//    val student1: DataFrame = Seq(Student1(1, "bj", "zhangsan", 21),
//      Student1(2, "bj", "lisi", 22),
//      Student1(3, "sh", "wangwu", 23),
//      Student1(4, "tj", "zhaoliu", 24),
//      Student1(5, "sh", "tianqi", 25)
//    ).toDF()

    val df1: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],Encoders.product[Student1].schema)
    val df2: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],Encoders.product[Student].schema)
    val df: DataFrame  = df1.join(df2,Seq("id"),"LEFT").withColumn("DATE",lit("2019-12-12"))
    if (df.toJavaRDD.isEmpty()){
      df.write.mode("overwrite").orc("")
    }
//    println("===========")
//    person.join(student, Seq("id"), "outer").show()
//    println("two_id")
//    person.join(student, person("id") === student("id"), "outer").show()
//    println("spark data handle success")

//    val student = Seq(Student(1, "zhangsan", "zhangbj", 11),
//      Student(2, "lisi", "libj", 12),
//      Student(3, "wangwu", "wangsh", 13),
//      Student(4, "zhaoliu", "zhaotj", 14),
//      Student(6, "tianqi", "tiansh", 15)
//    ).toDS()
//
//    val student1 = Seq(Student1(1, "bj", "zhangsan", 21),
//      Student1(2, "bj", "lisi", 22),
//      Student1(3, "sh", "wangwu", 23),
//      Student1(4, "tj", "zhaoliu", 24),
//      Student1(5, "sh", "tianqi", 25)
//    ).toDS()

//    val unit: Dataset[Student] = student.union(student)

//        student1.unionByName(student).show()
//        student.union(student1).show()
    //    spark.close()
    //    System.exit(0)
  }
}
