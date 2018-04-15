package com.xlf.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SQLContext使用
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {

    val path = args(0)
    //创建响应的Context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //相关处理
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //关闭资源
    sc.stop()
  }
}
