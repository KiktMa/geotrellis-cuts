//package com.spark.demo.pointcloud
//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
//import org.apache.hadoop.io.Text
//import org.apache.hadoop.mapreduce.Job
//
//object LASBatchCutter {
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("LASBatchCutter")
//    val sc = new SparkContext(conf)
//    val spark = SparkSession.builder().appName("LASBatchCutter").getOrCreate()
//
//    val inputPath = args(0) // 输入LAS文件的路径
//    val tileSize = args(1).toInt // 块的大小，单位为米
//    val instanceName = args(2) // Accumulo实例的名称
//    val zookeeper = args(3) // ZooKeeper的地址
//    val user = args(4) // Accumulo用户名
//    val password = args(5) // Accumulo密码
//    val tableName = args(6) // 存储LAS数据的表格名称
//
//    // 读取输入LAS文件
//    val lasData = spark.read.format("com.github.sadikovi.spark.io.las").load(inputPath)
//
//    // 获取LAS文件的边界范围
//    val bounds = lasData.agg(
//      min($"X"), max($"X"), min($"Y"), max($"Y")
//    ).collect()(0)
//
//    val minX = bounds.getDouble(0)
//    val maxX = bounds.getDouble(1)
//    val minY = bounds.getDouble(2)
//    val maxY = bounds.getDouble(3)
//
//    // 计算切割后的块数
//    val numCols = math.ceil((maxX - minX) / tileSize).toInt
//    val numRows = math.ceil((maxY - minY) / tileSize).toInt
//
//    // 将LAS数据转换为Accumulo可接受的格式
//    val blockData = lasData
//      .select($"X", $"Y", $"Z", $"Intensity", $"ReturnNumber", $"NumberOfReturns", $"ScanDirectionFlag", $"EdgeOfFlightLine", $"Classification")
//      .withColumn("X", floor($"X" / tileSize) * tileSize)
//      .withColumn("Y", floor($"Y" / tileSize) * tileSize)
//      .withColumn("rowKey", concat($"X", lit("_"), $"Y").cast(StringType))
//      .withColumn("cf", lit("data"))
//      .withColumn("cq", lit(""))
//      .withColumn("value", concat_ws(",", $"X", $"Y", $"Z", $"Intensity", $"ReturnNumber", $"NumberOfReturns", $"ScanDirectionFlag", $"EdgeOfFlightLine", $"Classification"))
//      .select($"rowKey", $"cf", $"cq", $"value")
//
//    // 将块数据保存到Accumulo中
//    val job = Job.getInstance()
//    job.setOutputFormatClass(classOf[AccumuloOutputFormat])
//    job.getConfiguration.set(AccumuloOutputFormat.OUTPUT_TABLE, tableName)
//    job.getConfiguration.set(AccumuloOutputFormat.ZOOKEEPERS, zookeeper)
//    job.getConfiguration.set(AccumuloOutputFormat.INSTANCE_NAME, instanceName)
//    job.getConfiguration.set(AccumuloOutputFormat.USERNAME, user)
//    job.getConfiguration.set(AccumuloOutputFormat.PASSWORD, password)
//
//    blockData.rdd
//      .map(row => (new Text(row.getString(0)), row))
//      .saveAsNewAPIHadoopDataset(job.getConfiguration)
//
//    spark.stop()
//  }
//}
