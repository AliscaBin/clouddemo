package com.wangbin.clouddemo.util

import java.util

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._

//case class Person(id: Int, name: String, address: String)
//
//case class Student(id: Int, name: String, address: String, high: Int)

//case class Student1(id: Int, address: String, name: String, high: Int)


object testalias {
  def main(args: Array[String]): Unit = {
    println("get the SparkSession")
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._
    println("create the DataFrame")
    val person: DataFrame = Seq(Person(1, "11", "1"),
      Person(2, "12", "2"),
      Person(3, "13", "3"),
      Person(4, "14", "4"),
      Person(5, "15", "5")
    ).toDF()
    val person1: Dataset[Row] = person.as("a")
    val person2: Dataset[Row] = person.alias("bb")
    println("-------first test--------")
    person1.join(person2,$"A.id" === $"bb.id","left").show(false)
//    +---+--------+-------+---+--------+-------+
//    |id |name    |address|id |name    |address|
//    +---+--------+-------+---+--------+-------+
//    |1  |zhangsan|zhangbj|1  |zhangsan|zhangbj|
//    |2  |lisi    |libj   |2  |lisi    |libj   |
//    |3  |wangwu  |wangsh |3  |wangwu  |wangsh |
//    |4  |zhaoliu |zhaotj |4  |zhaoliu |zhaotj |
//    |5  |tianqi  |tiansh |5  |tianqi  |tiansh |
//    +---+--------+-------+---+--------+-------+

    val person3: Dataset[Row] = person.filter($"id" =!= 1).as("a")
    val person4: Dataset[Row] = person.alias("bb")
    println("-------second test--------")
    person3.join(person4,$"a.id" === $"bb.id","left").show(false)
//    +---+-------+-------+---+-------+-------+
//    |id |name   |address|id |name   |address|
//    +---+-------+-------+---+-------+-------+
//    |2  |lisi   |libj   |2  |lisi   |libj   |
//    |3  |wangwu |wangsh |3  |wangwu |wangsh |
//    |4  |zhaoliu|zhaotj |4  |zhaoliu|zhaotj |
//    |5  |tianqi |tiansh |5  |tianqi |tiansh |
//    +---+-------+-------+---+-------+-------+


//    person1.join(person3,$"a.id"===$"a.id","left").show(false)
//    person1.join(person3,$"person1.id"===$"a.id","left").show(false)
//    Exception in thread "main" org.apache.spark.sql.AnalysisException: Reference 'a.id' is ambiguous, could be: a.id, a.id.;
//
    println("-------third test--------")
    person1.filter($"id" =!= 4).as("a").join(person3.as("b"),$"a.id"===$"b.id","left").show(false)
//    +---+--------+-------+----+-------+-------+
//    |id |name    |address|id  |name   |address|
//    +---+--------+-------+----+-------+-------+
//    |1  |zhangsan|zhangbj|null|null   |null   |
//    |2  |lisi    |libj   |2   |lisi   |libj   |
//    |3  |wangwu  |wangsh |3   |wangwu |wangsh |
//    |5  |tianqi  |tiansh |5   |tianqi |tiansh |
//    +---+--------+-------+----+-------+-------+
    println("-------second test--------")
    person.select($"name" -2 as "name").show()
    person.select($"name" + $"address" as "result").show()
    person.select(concat($"name",lit("-"),$"address")  as "result").show()



    val idplus = udf((name: String) => {
      person.filter($"name" === name).select($"id"+1).first().apply(0).toString()
    })

    person1.select(idplus($"name") as "idplus").show()
//    val student: DataFrame = Seq(Student(1, "zhangsan", "zhangbj", 11),
//      Student(2, "lisi", "libj", 12),
//      Student(3, "wangwu", "wangsh", 13),
//      Student(4, "zhaoliu", "zhaotj", 14),
//      Student(6, "tianqi", "tiansh", 15)
//    ).toDF()
//
//    println ("spark result :" + $"aa.id")
  }
}
