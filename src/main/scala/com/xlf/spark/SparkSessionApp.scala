package com.xlf.spark

import org.apache.spark.sql.SparkSession

object SparkSessionApp {
  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder().appName("SparkSesssionApp").master("local[2]").getOrCreate()
    //相关处理
    val people = spark.read.json("file:///Users/xiaoliufu/data/people.json")
    people.show()



    //关闭资源
    spark.stop()
  }
}
