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
    spark.sparkContext.setLogLevel("ERROR")
    println("create the DataFrame")
    val person: DataFrame = Seq(Person(1, "zhangsan", "zhangbj"),
      Person(2, "lisi", "libj"),
      Person(3, "wangwu", "wangsh"),
      Person(4, "zhaoliu", "zhaotj"),
      Person(5, "tianqi", "tiansh")
    ).toDF()

person.select(substring($"name",1,3) as "name").show()
//    person.select(
//      when(
//        when($"id" < 4, 6)
//          .when($"id" < 5, 5)
//          .otherwise(1) <= 6 and
//          when($"name".isin("zhangsan", "lisi"), "wangbin")
//            .otherwise($"name") === "wangwu", 1)
//        .otherwise(0) as "id",
//      $"name" as "name"
//    ).show()
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

    val df1: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Encoders.product[Student1].schema)
    val df2: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Encoders.product[Student].schema)
    val df: DataFrame = df1.join(df2, Seq("id"), "LEFT").withColumn("DATE", lit("2019-12-12"))
    //    if (df.toJavaRDD.isEmpty()){
    //      df.write.mode("overwrite").orc("")
    //    }
    //    println("===========")
    //    person.join(student, Seq("id"), "outer").show()
    //    println("two_id")
    //    person.join(student, person("id") === student("id"), "outer").show()
    //    println("spark data handle success")

        val student: Dataset[_] = Seq(Student(1, "zhangsan", "zhangbj", 11),
          Student(2, "lisi", "libj", 12),
          Student(3, "wangwu", "wangsh", 13),
          Student(4, "zhaoliu", "zhaotj", 14),
          Student(6, "tianqi", "tiansh", 15)
        ).toDS()

    val student1: Dataset[_] = Seq(Student1(1, "bj", "zhangsan", 21),
      Student1(2, "bj", "lisi", 22),
      Student1(3, "sh", "wangwu", 23),
      Student1(4, "tj", "zhaoliu", 24),
      Student1(5, "sh", "tianqi", 25)
    ).toDS()


    //    val unit: Dataset[Student] = student.union(student)

//            student1.toDF().unionByName(student.toDF()).show()
    /**
      * 针对Dataset 调用api unionByName 会进行类型检测，类型不匹配则报错
      *
      * 解决方法：将Dataset转为Dataframe 调用api unionByName 即可
      *
      * 靠谱的解决方法还是创建一个case class 施加在两个数据集上，方便字段的校验
      */
//            student.unionByName(student1).show()
    /**上面调用unionByName报错，错误信息如下：
      * Error:(83, 33) type mismatch;
      * found   : org.apache.spark.sql.Dataset[_$2] where type _$2
      * required: org.apache.spark.sql.Dataset[_$1]
      * student.unionByName(student1).show()
      */

    //        student.union(student1).show()
    //    spark.close()
    //    System.exit(0)
  }
}
