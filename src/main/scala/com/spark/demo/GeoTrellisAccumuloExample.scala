package com.spark.demo

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.accumulo.AccumuloInstance
import geotrellis.spark.io.index.{KeyIndex, ZCurveKeyIndexMethod}
import geotrellis.spark.pyramid.Pyramid
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{ClientConfiguration, ZooKeeperInstance}
import org.apache.spark._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD


object GeoTrellisAccumuloExample {
  def main(args: Array[String]): Unit = {

    val instanceName = "accumulo"
    val zookeepers = "node1:2181,node2:2181,node3:2181"
    val user = "root"
    val password = "root"
    val table = "tiles"
    val resampleMethod = Bilinear

    val conf = new Configuration()
    implicit val sc = new SparkContext(new SparkConf().setAppName("GeoTrellis-HDFS-Accumulo").setMaster("yarn"))

    val path = new Path("hdfs://node1:9000/tiff/caijian_zhuanhuan.tif")
    val geoRDD = sc.hadoopGeoTiffRDD(path)

    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(geoRDD, FloatingLayoutScheme(512,512))
    val tiled = geoRDD.tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, resampleMethod)


    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 512)
    val (18 /*maxZoom*/, reprojected) = TileLayerRDD(tiled, rasterMetaData)
      .reproject(WebMercator, layoutScheme, Bilinear)

    Pyramid.upLevels(reprojected, layoutScheme, 8, Bilinear) { (rdd, z) =>
      // val landsatId = LayerId("instanceFileName", z)
      val writer = AccumuloLayerWriter(AccumuloInstance.apply(instanceName, zookeepers, user, new PasswordToken(password)), table)
      val layerId = LayerId("layer_name", z)

//      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
    }
  }
}