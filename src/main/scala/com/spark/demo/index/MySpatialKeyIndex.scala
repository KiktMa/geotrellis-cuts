package com.spark.demo.index

import com.spark.demo.submit.Etl2Accumulo.hadoopInPath
import geotrellis.proj4.WebMercator
import geotrellis.raster.Tile
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.{SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata, TileLayerRDD, withTilerMethods}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloInstance, AccumuloLayerDeleter, AccumuloLayerWriter, AccumuloWriteStrategy, HdfsWriteStrategy}
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.vector.{Point, ProjectedExtent}
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, PasswordToken}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object MySpatialKeyIndex {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SelfIndex")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    val sparkContext = new SparkContext(conf)
    val hadoopInPath = args(0)
    val instanceName: String = "accumulo"
    val zookeeper: String = "node1:2181,node2:2181,node3:2181"
    val user: String = "root"
    val token: AuthenticationToken = new PasswordToken("root")
    val dataTable: String = args(1)

    val strat: AccumuloWriteStrategy = HdfsWriteStrategy(new Path(hadoopInPath))

    val opts: AccumuloLayerWriter.Options = AccumuloLayerWriter.Options(strat)

    implicit val instance: AccumuloInstance = AccumuloInstance(
      instanceName,
      zookeeper,
      user,
      token
    )
    val attributeStore = AccumuloAttributeStore(instance)

    val writer = AccumuloLayerWriter(instance, dataTable, opts)

    val geoRDD: RDD[(ProjectedExtent, Tile)] = sparkContext.hadoopGeoTiffRDD(hadoopInPath)

    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(geoRDD, FloatingLayoutScheme(256))

    val tiled: RDD[(SpatialKey, Tile)] = geoRDD.
      tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)

    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator, 512)

    val (maxZoom, reprojected) = TileLayerRDD(tiled, rasterMetaData)
      .reproject(WebMercator, layoutScheme, Bilinear)

    Pyramid.upLevels(reprojected, layoutScheme, args(2).toInt, args(3).toInt) { (rdd, z) =>
      val layerId = LayerId("layer_" + dataTable, z)

//      val metadata = rdd.metadata
//      val resolution = metadata.layout.cellSize.resolution
//      rdd.foreach( tile => {
//          val extent = metadata.layout.mapTransform(tile._1)
//          val resolution = metadata.layout.cellSize.resolution
//          val col = tile._1.col
//          val row = tile._1.row
//          val x = extent.xmin + (col + 0.5) * metadata.layout.tileCols * resolution
//          val y = extent.ymax - (row + 0.5) * metadata.layout.tileRows * resolution
//          val point = Point(x, y)
//          val code = GeoSot.INSTANCE.getHexCode(point.x, point.y, 0, z)
//          (tile._1, tile._2, code)
//          GeoSot.INSTANCE.freeMemory(code)
//      })

      if (attributeStore.layerExists(layerId)) {
        AccumuloLayerDeleter(attributeStore).delete(layerId)
      }

      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
    }
  }
}

