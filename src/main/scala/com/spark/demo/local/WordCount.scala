package com.spark.demo.local

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * windows环境下spark的wordcount快速入门程序
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Read").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("datas")

    val words = lines.flatMap(_.split(" "))

    val word2One = words.map(word => (word, 1))

    val wordToCount = word2One.reduceByKey(_ + _)

    val array = wordToCount.collect()

    array.foreach(println)
  }
}
