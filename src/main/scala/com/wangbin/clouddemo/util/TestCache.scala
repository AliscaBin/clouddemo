package com.wangbin.clouddemo.util
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestCache {
  def main(args: Array[String]): Unit = {
    println("get the SparkSession")
    val spark: SparkSession = SparkBuilder.builder(true)
    import spark.implicits._
    println("create the DataFrame")
    val person: DataFrame = Seq(Person(1, "zhangsan", "zhangbj"),
      Person(2, "lisi", "libj"),
      Person(3, "wangwu", "wangsh"),
      Person(4, "zhaoliu", "zhaotj"),
      Person(5, "tianqi", "tiansh")
    ).toDF()

    val person1: DataFrame = Seq(Person(6, "zhangsan", "zhangbj"),
      Person(7, "lisi", "libj"),
      Person(8, "wangwu", "wangsh"),
      Person(9, "zhaoliu", "zhaotj"),
      Person(10, "tianqi", "tiansh")
    ).toDF()

    person1.cache()
    person1.show()
    person1.show()
    person1.show()

//    for (i<-1 to 5){
//      println("----------------------------------------------------")
//      person.join(person1,$"person.id"===$"person1.id","left").show()
//    }

  }
}
