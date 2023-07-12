//package com.spark.demo.local
//
//import org.apache.avro.io.Encoder
//import org.apache.spark.sql.SparkSession
//case class Point(x: Double, y: Double, z: Double)
//
//object CutPointClouds{
//
//  private val session: SparkSession = SparkSession.builder().appName("Cut").master("local[*]").getOrCreate()
//
//  def main(args: Array[String]): Unit = {
//
//    session.read.textFile("C:\\Users\\mj\\Desktop\\paper\\pointcloud\\corrected-LJYY-Cloud-1-0-9 - Cloud.txt")
//      .map(line => {
//
//      })(Encoder[Unit])
//    // Example usage
//    val pointCloudData = Array(
//      Point(1.2, 3.4, 5.6),
//      Point(2.1, 4.3, 6.5)
//      // Add more point data here...
//    )
//
//    val stepSize = 0.1 // Specify your desired step size
//    val cubeSize = 1.0 // Specify the size of each cube
//
//    val splitPointCloudData: Map[(Int, Int, Int), Array[Point]] = splitPointCloud(pointCloudData, stepSize, cubeSize)
//
//    println(splitPointCloudData.toList)
//  }
//
//  def splitPointCloud(pointCloud: Array[Point], step: Double, cubeSize: Double): Map[(Int, Int, Int), Array[Point]] = {
//    val minPoint = Point(pointCloud.map(_.x).min, pointCloud.map(_.y).min, pointCloud.map(_.z).min)
//    val maxPoint = Point(pointCloud.map(_.x).max, pointCloud.map(_.y).max, pointCloud.map(_.z).max)
//
//    val splitRanges = for {
//      x <- Range.Double(minPoint.x, maxPoint.x, cubeSize)
//      y <- Range.Double(minPoint.y, maxPoint.y, cubeSize)
//      z <- Range.Double(minPoint.z, maxPoint.z, cubeSize)
//    } yield ((x / step).toInt, (y / step).toInt, (z / step).toInt)
//
//    val groupedPoints = pointCloud.groupBy { point =>
//      ((point.x / step).toInt, (point.y / step).toInt, (point.z / step).toInt)
//    }
//
//    splitRanges.map(range => range -> groupedPoints.getOrElse(range, Array.empty)).toMap
//  }
//}