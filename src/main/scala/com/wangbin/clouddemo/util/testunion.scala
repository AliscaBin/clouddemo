package com.wangbin.clouddemo.util

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
case class Person(id:Int,name:String,address:String)
case class Student(id:Int,name:String,address:String,high:Int)
case class Student1(id:Int,address:String,name:String,high:Int)



object testunion {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkBuilder.builder()
    import spark.implicits._
    val person: DataFrame = Seq(Person(1, "zhangsan", "zhangbj"),
      Person(2, "lisi", "libj"),
      Person(3, "wangwu", "wangsh"),
      Person(4, "zhaoliu", "zhaotj"),
      Person(5, "tianqi", "tiansh")
    ).toDF()

    val student: DataFrame = Seq(Student(1, "zhangsan", "zhangbj",11),
      Student(2, "lisi", "libj",12),
      Student(3, "wangwu", "wangsh",13),
      Student(4, "zhaoliu", "zhaotj",14),
      Student(5, "tianqi", "tiansh",15)
    ).toDF()

    val student1: DataFrame = Seq(Student1(1, "bj", "zhangsan",21),
      Student1(2, "bj","lisi", 22),
      Student1(3, "sh","wangwu", 23),
      Student1(4, "tj","zhaoliu", 24),
      Student1(5, "sh","tianqi", 25)
    ).toDF()

    student1.unionByName(student).show()
    student.union(student1).show()
    println("stopstopstopstopstopstopstopstopstopstopstopstopstop")
    println("stopstopstopstopstopstopstopstopstopstopstopstopstop")
    println("stopstopstopstopstopstopstopstopstopstopstopstopstop")
    println("stopstopstopstopstopstopstopstopstopstopstopstopstop")
    println("stopstopstopstopstopstopstopstopstopstopstopstopstop")
    println("stopstopstopstopstopstopstopstopstopstopstopstopstop")
    spark.close()
    System.exit(0)
  }
}
