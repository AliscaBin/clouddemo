package com.wangbin.clouddemo.util

object splitString {

  def main(args: Array[String]): Unit = {
    val str = "Stringaaaisaaaobject"
    val strings: Array[String] = str.split("aaa")
    println(strings.mkString(","))
  }

}
