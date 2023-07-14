package com.spark.demo.connect

import org.apache.spark.{SparkConf, SparkContext}

object SparkConnector {
  private val APP_NAME = "MySparkApp"
  private val MASTER = "yarn"

  def createSparkContext(): SparkContext = {
    // 创建spark连接配置
    val conf = new SparkConf()
      .setAppName(APP_NAME)
      .setMaster(MASTER)

    new SparkContext(conf)
  }
}
