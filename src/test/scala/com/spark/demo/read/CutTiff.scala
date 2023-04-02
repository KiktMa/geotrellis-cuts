package com.spark.demo.read

import com.spark.demo.Cuts.hadoopInPath
import geotrellis.proj4.WebMercator
import geotrellis.raster.Tile
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, SinglebandGeoTiff, Tiled}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample.Bilinear
import geotrellis.spark._
import geotrellis.spark.io.{SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloInstance, AccumuloLayerWriter, AccumuloValueReader}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.vector.ProjectedExtent
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CutTiff {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("TiffSlicer")
//    val sc = new SparkContext(conf)
//
//    // 设置Accumulo连接参数
//    val accumuloInstance = "myinstance"
//    val accumuloUser = "root"
//    val accumuloPassword = new PasswordToken("password")
//    val accumuloZookeeper = "zookeeper.example.com"
//    val accumuloTable = "tiles"
//    val indexTableName = s"${accumuloTable}_idx"
//    val instance = AccumuloInstance(accumuloInstance, accumuloZookeeper, accumuloUser, accumuloPassword)
//    val tileLayerWriter = AccumuloLayerWriter(instance,indexTableName)
//
//    // 设置瓦片大小和缩放级别
//    val tileSize = 256
//    val layoutScheme = ZoomedLayoutScheme(WebMercator)
//
//    // 从HDFS中读取TIFF文件
//    val tifPath = new Path("hdfs://localhost:9000/input.tif")
//    val tif: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(tifPath.toString)
//
//    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(tif, FloatingLayoutScheme(512))
//
//    // 将TIFF文件转换为TileLayerRDD
//    val (zoom, rdd) = TileLayerRDD(tif, rasterMetaData)
//
//    // 将TileLayerRDD切分为瓦片，并将每个瓦片写入Accumulo数据库中
//    rdd.foreachPartition { partition =>
//      val writeOpts = new AccumuloWriteOptions()
//      partition.foreach { case (key, tile) =>
//        val tileBytes = tile.toBytes()
//        val row = s"${key.col}_${key.row}"
//        val colFamily = s"${key.zoom}"
//        val colQualifier = s"${key.col}"
//        val tileId = s"$row:$colFamily:$colQualifier"
//        tileLayerWriter.write(accumuloTable, tileId, tileBytes, writeOpts)
//      }
//    }
//    // 创建和写入元数据索引表
//    val metadata = TileLayerMetadata(zoom, tif.extent, tif.cellType, layoutScheme, tileSize)
//    tileLayerWriter.writeMetaData(indexTableName, metadata)

    // 初始化Spark上下文
    val conf = new SparkConf().setAppName("TifToAccumulo")
    implicit val sc = SparkContext.getOrCreate(conf)

    // HDFS路径和读取参数
    val hdfsPath = new Path("hdfs://localhost:9000/path/to/tif")

    // 从HDFS中读取GeoTiff
    val geoRDD: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(hdfsPath)

    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(geoRDD, FloatingLayoutScheme(512))

    // 创建空间参考系和瓦片布局规则
    val crs = rasterMetaData.crs
    val tileLayoutScheme = ZoomedLayoutScheme(crs, tileSize = 256)

    // 切分GeoTiff为瓦片
    val tiled: RDD[(SpatialKey, Tile)] = geoRDD.
      tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
    // 请特别注意，此处咨询过 GeoTrellis 的作者，请不要随意的使用repartition!!
    // repartition 会将所有的数据进行重新分区，增加计算量！
    //  .repartition(100)

    //    设置投影和瓦片大小
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 512)

    val (_, reprojected) = TileLayerRDD(tiled, rasterMetaData)
      .reproject(WebMercator, layoutScheme, Bilinear)

    // 创建Accumulo实例和图层写入器
    val accumuloInstance = AccumuloInstance(
      "myinstance",
      "localhost:2181",
      "root",
      new PasswordToken("password")
    )

    val writer = AccumuloLayerWriter(accumuloInstance,"")

    // 将瓦片存储到Accumulo中
    val layerId = LayerId("mylayer", 18)
    writer.write(layerId, reprojected, ZCurveKeyIndexMethod)

    // 关闭Spark上下文
    sc.stop()
  }
}
