package com.spark.demo.submit

import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{ByteConstantNoDataCellType, Tile}
import geotrellis.spark.io.{SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloInstance, AccumuloLayerDeleter, AccumuloLayerWriter}
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

   /**
   * 这里在提交spark任务时需要加入两个参数传入main中的args
   * @param args args(0)表示栅格数据存储在hdfs中的位置
   *             args(1)表示将金字塔模型存储到accumulo数据库中表名
   */

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("GeoTrellis2Accumulo")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
    implicit val sparkContext: SparkContext = new SparkContext(sparkConf)

    val path = new Path(args(0))
    val geoTiff = HadoopGeoTiffRDD.spatial(path)

    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(geoTiff, FloatingLayoutScheme(512))

    val CoverLayer: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(
      geoTiff.tileToLayout(rasterMetaData, Bilinear)
        .mapValues { tile => tile.convert(ByteConstantNoDataCellType) },
      rasterMetaData.copy(cellType = ByteConstantNoDataCellType))

    val layerId = LayerId("layer_"+args(1), 18)

    val writer = AccumuloLayerWriter(AccumuloInstance(
      instance, zookeepers, user, new PasswordToken(passwordToken)), args(1))

    val attributeStore = AccumuloAttributeStore(AccumuloInstance(
      instance, zookeepers, user, new PasswordToken(passwordToken)))

    if (attributeStore.layerExists(layerId)) {
      AccumuloLayerDeleter(attributeStore).delete(layerId)
    }
    writer.write(layerId,CoverLayer,ZCurveKeyIndexMethod)
  }
}