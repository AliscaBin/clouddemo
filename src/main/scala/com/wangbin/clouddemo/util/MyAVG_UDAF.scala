package com.wangbin.clouddemo.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object MyAVG_UDAF extends UserDefinedAggregateFunction{
  //定义输入的类型
  override def inputSchema: StructType = StructType(StructField("input",LongType)::Nil)
  //定义缓存中间值类型(计算平均数，第一个位sum总数，第二个位count个数)
  override def bufferSchema: StructType = StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)
  //定义输出的类型
  override def dataType: DataType = StructType(StructField("output",DoubleType)::Nil)
  //函数的稳定参数：true不受时间戳的影响而改变结果
  override def deterministic: Boolean = true

  //初始化缓存中的初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }
  //更新（Executor内生效）
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }
  //合并（Executor间生效）
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    val avg = buffer.getLong(0).toDouble / buffer.getLong(1)
    avg
  }

  /**
   * 使用时需要注册
   * spark.udf.register("funcName",MyAVG_UDAF)
   */
}
