package com.xlf.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * hiv
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {
    //创建相应的Context
    val sparkConf = new SparkConf()

    //在测试或者生产中，AppName和Master我们是通过脚本进行制定
//    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //相关处理
    hiveContext.table("emp").show()

    //关闭资源
    sc.stop()
  }
}
