package com.wangbin.clouddemo.util

object ArrayTest {
  def main(args: Array[String]): Unit = {
    println("".split(",").toSeq.isEmpty)
    println(!Seq().isEmpty)
    println(!Array().isEmpty)
  }
}
