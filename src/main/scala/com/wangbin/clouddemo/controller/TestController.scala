package com.wangbin.clouddemo.controller

import java.lang

import com.wangbin.clouddemo.util.SparkBuilder
import org.apache.spark.sql.Dataset
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{GetMapping, ResponseBody}

@Controller
class TestController {

  @GetMapping(Array("/hello"))
  @ResponseBody
  def hello(): Unit ={
    val spark = SparkBuilder.builder(true)
    val df: Dataset[lang.Long] = spark.range(10,100)
    df.show()
    println(spark)
    println("/hello")
  }

  @GetMapping(Array("/info"))
  @ResponseBody
  def hello2():Unit = {
    val spark = SparkBuilder.builder(true)
    val df: Dataset[lang.Long] = spark.range(10,100)
    df.show()
    println(spark)
    println("/info")
  }

}
