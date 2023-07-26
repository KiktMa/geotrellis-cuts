package com.spark.demo.local

import com.geosot.javademo.geosot.{ChangeCode, CoordinateChange}
import com.spark.demo.index.GeoSot
import org.apache.spark.sql.SparkSession

/**
 * 将点云转成txt格式后，为每个点多添加三个属性
 * 分别为：点的经度、点的纬度、点的geosot编码
 * (x, y, z, field1, field2, field3, lonlat(0), lonlat(1), geocode)
 */
object SparkFileProcessing {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Text File Operation")
      .getOrCreate()

    // 读取文本文件
    val inputPath = "D:\\JavaConsist\\MapData\\180m_pointcloud\\0\\corrected-LJYY-Cloud-1-0-0 - Clean.txt"
    val outputPath = "D:\\JavaConsist\\MapData\\180m_pointcloud\\0\\new\\corrected-LJYY-Cloud-1-0-0 - Clean.txt"

    val data = spark.read.textFile(inputPath)

    import spark.implicits._

    // 对每行的点数据都添加另外三列信息：经度纬度以及geosot编码
    val result = data.map(line => {
      val values = line.split(" ").map(_.toDouble)
      val x = values(0)
      val y = values(1)
      val z = values(2)
//      val field1 = values(3)
//      val field2 = values(4)
//      val field3 = values(5)
      val lonlat = CoordinateChange.getCoordinate(x, y)
      val geoarr = new Array[Byte](32)
      GeoSot.INSTANCE.PointGridIdentify3D(lonlat(0), lonlat(1), z, 32: Byte, geoarr)
      val code: ChangeCode = new ChangeCode(geoarr, 32)
      val geocode: String = code.getHexOneDimensionalCode
      (x, y, z, lonlat(0), lonlat(1), geocode)
    })
    val combinedResult = result.coalesce(1)
    // 将结果写回到原始文件
    combinedResult.map { case (x, y, z, lon, lat, geocode) =>
      s"$x $y $z $lon $lat $geocode"
    }.write.text(outputPath)

    // 关闭 SparkSession
    spark.stop()
  }
}
