package com.spark.demo

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffTileMethods, SinglebandGeoTiff}
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io.{SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.accumulo.AccumuloInstance
import geotrellis.spark.io.index.{KeyIndex, ZCurveKeyIndexMethod}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector.ProjectedExtent
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{ClientConfiguration, ZooKeeperInstance}
import org.apache.spark._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD


object GeoTrellisAccumulo {

  // Accumulo database parameters
  val accumuloInstance = "accumulo"
  val zookeepers = "node1:2181,node2:2181,node3:2181"
  val user = "root"
  val password = "root"
  val table = "test"

  // Tile size in pixels
  val tileSize = 512
  def main(args: Array[String]): Unit = {

//    // 初始化Spark上下文
    val conf = new SparkConf().setAppName("TifToAccumulo")
    implicit val sc = SparkContext.getOrCreate(conf)
//
//    // HDFS路径和读取参数
//    val hdfsPath = new Path("hdfs://node1:9000/tiff/caijian_zhuanhuan.tif")
//
//    // 从HDFS中读取GeoTiff
//    val geoRDD: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(hdfsPath)
//
//    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(geoRDD, FloatingLayoutScheme(512))
//
//    // 切分GeoTiff为瓦片
//    val tiled: RDD[(SpatialKey, Tile)] = geoRDD.
//      tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
//
//    // 设置投影和瓦片大小
//    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 512)
//
//    val (_, reprojected) = TileLayerRDD(tiled, rasterMetaData)
//      .reproject(WebMercator, layoutScheme, Bilinear)
//
//    // 创建Accumulo实例和图层写入器
//    val accumuloInstance = AccumuloInstance(
//      "accumulo",
//      "node1:2181,node2:2181,node3:2181",
//      "root",
//      new PasswordToken("root")
//    )
//
//    val writer = AccumuloLayerWriter(accumuloInstance, "test")
//
//    // 将瓦片存储到Accumulo中
//    val layerId = LayerId("mylayer", 18)
//    writer.write(layerId, reprojected, ZCurveKeyIndexMethod)
//
//    // 关闭Spark上下文
//    sc.stop()

    // Create an Accumulo instance
    implicit val instance: AccumuloInstance =
      AccumuloInstance(accumuloInstance, zookeepers, user, new PasswordToken(password))

    // Read the GeoTiff file from HDFS
    val uri = new Path("hdfs://node1:9000/tiff/caijian_zhuanhuan.tif")
    val geoTiff: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(uri)

    // Convert the GeoTiff to a RasterTile
    val raster = geoTiff.rdd

    // Define the layer name and zoom level
    val layerName = "my-layer"
    val zoomLevel = 18

    // Define the layout scheme based on the zoom level and tile size
    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize)

    // Generate a layout for the given raster extent using the layout scheme
    val layout = layoutScheme.levelForZoom(zoomLevel).layout

    // Cut the raster into tiles that align with the layout using the specified index method
    val tiledRaster = ContextRDD(raster, layout)
        .tileToLayout(TileLayerMetadata.fromRDD(geoTiff,layout),
          Tiler.Options(resampleMethod = NearestNeighbor))

    // Create an Accumulo layer writer for the given table
    val writer = AccumuloLayerWriter(instance, table)

    // Write the tiles to the database with row key (column, row, zoom level) and metadata (extent, crs)
    writer.write(LayerId(layerName,zoomLevel), tiledRaster,ZCurveKeyIndexMethod)
  }
}