package com.spark.demo.pointcloud

import org.apache.spark.sql.SparkSession

object LASBatchCutter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LAS Reader").getOrCreate()

    val dataFrame = spark.read.format("parquet").load("D:\\JavaConsist\\MapData\\180m_pointcloud\\corrected-LJYY-Cloud-1-0-0.las")

  }
}
