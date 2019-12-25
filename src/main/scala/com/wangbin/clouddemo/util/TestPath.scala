package com.wangbin.clouddemo.util

import java.net.URL

object TestPath {
  def main(args: Array[String]): Unit = {
    val resource1: URL = TestPath.getClass.getClassLoader.getResource("")
    val resource2: URL = TestPath.getClass.getResource("")
    val resource3: URL = TestPath.getClass.getResource("/")
    println("resource1 => " + resource1)
    println("resource2 => " + resource2)
    println("resource3 => " + resource3)
    val mypath = System.getProperty("user.dir").replace("\\", "/") + "/test_data"
    println(mypath)
//    D:\IdeaProjects\bohai_credit_report_20191028\bohai_credit_batch\test_data

    println(System.getProperty("user.dir")+"/test_date")


    println("111|23|".split("\\|",-1).length)

  }
}
