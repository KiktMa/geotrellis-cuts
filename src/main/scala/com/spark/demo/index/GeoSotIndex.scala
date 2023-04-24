//package com.spark.demo.index
//
//import geotrellis.spark.KeyBounds
//import geotrellis.spark.io.index.{KeyIndex, KeyIndexMethod}
//
//import scala.io.Source
//import scala.math.{floor, pow}
//
//case class DegreeMinuteSecond(degrees: Int, minutes: Int, seconds: Double) {
//  override def toString: String = s"${degrees}° ${minutes}' ${seconds}\""
//}
//
//// 转换度分秒格式
//def convertToDegreeMinuteSecond(d: Double): DegreeMinuteSecond = {
//  val degrees = floor(d).toInt
//  val minutes = floor((d - degrees) * 60).toInt
//  val seconds = (d - degrees - minutes.toDouble / 60) * 3600
//  DegreeMinuteSecond(degrees, minutes, seconds)
//}
//
//// 转换 GeoSOT 编码值
//def convertToGeoSOTCode(dms: DegreeMinuteSecond): Long = {
//  val degInMs = dms.degrees * 64 * 64
//  val minInMs = dms.minutes * 64
//  ((degInMs + minInMs + dms.seconds) * 2048).toLong
//}
//
//class GeoSotIndex extends KeyIndexMethod[String] {
//  override def createIndex(keyBounds: KeyBounds[String]): KeyIndex[String] = {
//    // 读取 txt 文件中的每一行字符串编码作为瓦片的索引
//    val filename = "your_file_path.txt"
//    val lines = Source.fromFile(filename).getLines().toList
//
//    // 计算单元面片大小
//    val level = 18 // 层级
//    val cellSize = pow(4, level - 1)
//
//    // 将字符串编码转换为瓦片索引
//    val tiles = lines.map { line =>
//      val Array(latitude, longitude) = line.split(",")
//
//      // 将经纬度转换为度分秒格式，然后转换为 GeoSOT 编码值
//      val latDMS = convertToDegreeMinuteSecond(latitude.toDouble)
//      val lonDMS = convertToDegreeMinuteSecond(longitude.toDouble)
//      val latGeoSOT = convertToGeoSOTCode(latDMS)
//      val lonGeoSOT = convertToGeoSOTCode(lonDMS)
//
//      // 转换为二进制，并在前面补 0，使长度为 level - 1
//      val latBinary = f"${latGeoSOT / cellSize}%${level - 1}s".replace(" ", "0")
//      val lonBinary = f"${lonGeoSOT / cellSize}%${level - 1}s".replace(" ", "0")
//
//      // 将二进制转换为四进制一维编码
//      val pGeoSOT = (0 until level - 1).map { i =>
//        latBinary(i).asDigit * 2 + lonBinary(i).asDigit
//      }.mkString("")
//
//      // 拼接G和剖分编码
//      s"G$pGeoSOT"
//    }.toArray
//
//    // 构建 KeyIndex
//    new GeoSotKeyIndex(tiles)
//  }
//}