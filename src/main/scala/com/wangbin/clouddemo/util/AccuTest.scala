package com.wangbin.clouddemo.util

import org.apache.spark.util.AccumulatorV2



class AccuTest extends AccumulatorV2[Int,Int]{
  var sum=0
  //是否为0
  override def isZero: Boolean = sum==0
  //复制
  override def copy(): AccumulatorV2[Int, Int] = {
    val accu = new AccuTest
    accu.sum=this.sum
    accu
  }
  // 重置
  override def reset(): Unit = sum=0
  //累加
  override def add(v: Int): Unit = sum=sum+v
  //合并
  override def merge(other: AccumulatorV2[Int, Int]): Unit = sum+=other.value

  override def value: Int = sum
}

/**
 * 自定义累加器使用时需要对累加器进行注册
 *
 * val accu = new AccuTest()
 * sc.register(accu)
 */
