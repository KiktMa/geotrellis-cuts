package com.spark.demo.submit

import geotrellis.raster.{ByteConstantNoDataCellType, Tile}
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark.io.{SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.io.accumulo.{AccumuloInstance, AccumuloLayerWriter}
import geotrellis.spark.{ContextRDD, LayerId, Metadata, SpatialKey, TileLayerMetadata, withTilerMethods}
import geotrellis.spark.io.hadoop.HadoopGeoTiffRDD
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.kryo.KryoRegistrator
import geotrellis.spark.tiling.FloatingLayoutScheme
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

object SinglebandIngest {

  val instance = "accumulo"
  val zookeepers = "node1:2181,node2:2181,node3:2181"
  val user = "root"
  val passwordToken = "root"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Submit2Accumulo")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
    implicit val sparkContext: SparkContext = new SparkContext(sparkConf)

    val path = new Path(args(0))
    val geoTiff = HadoopGeoTiffRDD.spatial(path)

    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(geoTiff, FloatingLayoutScheme(512))

    val CoverLayer: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(
      geoTiff.tileToLayout(rasterMetaData, NearestNeighbor)
        .mapValues { tile => tile.convert(ByteConstantNoDataCellType) },
      rasterMetaData.copy(cellType = ByteConstantNoDataCellType))

    val layerId = LayerId("layer_"+args(1), 18)

    val writer = AccumuloLayerWriter(AccumuloInstance(
      instance, zookeepers, user, new PasswordToken(passwordToken)), args(1))

    writer.write(layerId,CoverLayer,ZCurveKeyIndexMethod)
  }
}