package com.spark.demo.submit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.spark.demo.Cuts.{hadoopInPath, tileSize}
import geotrellis.layer.{LinkedCRSFormat, NamedCRSFormat, crsFormat, withCrsFormat}
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.io.{CellSizeFormat, CellTypeFormat}
import geotrellis.spark._
import geotrellis.spark.TileLayerMetadata.toLayoutDefinition
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo.{AccumuloInstance, AccumuloLayerWriter}
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import geotrellis.spark.pyramid._
import geotrellis.spark.tiling._
import geotrellis.vector._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.rdd.RDD

object GeoTiffPy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Pyramid Builder")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    implicit val sc = new SparkContext(conf)
    val tiffPath = "hdfs://node1:9000/tiff/caijian_zhuanhuan.tif"
    val rasterSourceRDD: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(tiffPath)
//    val zoomLevels = 18

    // 创建元数据信息
    val (_, metadata) = TileLayerMetadata.fromRDD(rasterSourceRDD, FloatingLayoutScheme(512))

    // 创建切片RDD
    val tiled: RDD[(SpatialKey, Tile)] = rasterSourceRDD.
      tileToLayout(metadata.cellType, metadata.layout, Bilinear)

    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator, 512)

    val (maxZoom, reprojected) = TileLayerRDD(tiled, metadata)
      .reproject(WebMercator, layoutScheme, Bilinear)

    val pyramid: Seq[(Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]])] = Pyramid.levelStream(
      reprojected, layoutScheme, 18, maxZoom).take(maxZoom).toSeq
    val accumuloInstance = "accumulo"
    val accumuloUser = "root"
    val zookeeper = "node1:2181,node2:2181,node3:2181"
    val accumuloPassword = new PasswordToken("root")
    val accumuloTable = "tiles"

    for ((zoom, layerMetadata) <- pyramid) {
      val layerId = LayerId(s"layerId",zoom)
      val layerRdd: RDD[(SpatialKey, Tile)] = rasterSourceRDD
        .tileToLayout(layerMetadata.metadata.cellType, toLayoutDefinition(layerMetadata.metadata), Bilinear)

      val accRdd = ContextRDD(layerRdd, layerMetadata.metadata)
      // 创建 Accumulo 实例并写入数据
      AccumuloLayerWriter(AccumuloInstance(
        accumuloInstance,zookeeper,accumuloUser,accumuloPassword),accumuloTable)
        .write(layerId, accRdd ,ZCurveKeyIndexMethod)
    }
    sc.stop()
  }
}