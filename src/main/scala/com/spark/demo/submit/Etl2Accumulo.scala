package com.spark.demo.submit

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.Boundable
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloInstance, AccumuloLayerDeleter, AccumuloLayerReader, AccumuloLayerWriter, AccumuloValueReader, AccumuloWriteStrategy, HdfsWriteStrategy}
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import geotrellis.spark.pyramid._
import geotrellis.spark.tiling._
import geotrellis.vector._
import monocle.PLens
import org.apache.accumulo.core.client.{ClientConfiguration, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, PasswordToken}
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.rdd.RDD
object Etl2Accumulo {
  // 输入文件路径
  private val hadoopInPath: String = "hdfs://node1:9000/tiff/caijian_zhuanhuan.tif"
  // 输出文件路径
  private val outFilePath: String = "/app/tif"

  // spark Master
  private val master: String = "yarn"
  // spark 应用名称
  private val appName: String = "GeoTrellis2Accumulo"

  private val sparkConf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(master)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

  val sparkContext = new SparkContext(sparkConf)

  /**
   * 这个函数中的方法并没有进行封装，自己测试的时候用的
   * @param args
   */
  def main(args: Array[String]): Unit = {
    try {
      run(sparkContext)
    } finally {
      sparkContext.stop()
    }
  }
  def run(implicit sparkContext: SparkContext): Unit = {

    val instanceName: String = "accumulo"
    val zookeeper: String = "node1:2181,node2:2181,node3:2181"
    val user: String = "root"
    val token: AuthenticationToken = new PasswordToken("root")
    val dataTable: String = "tiles"

    val strat: AccumuloWriteStrategy = HdfsWriteStrategy(new Path(hadoopInPath))

    val opts: AccumuloLayerWriter.Options = AccumuloLayerWriter.Options(strat)

    implicit val instance: AccumuloInstance = AccumuloInstance(
      instanceName,
      zookeeper,
      user,
      token
    )
    val attributeStore = AccumuloAttributeStore(instance)
    //    val store: AttributeStore = AccumuloAttributeStore(instance)
    //    val reader = AccumuloLayerReader(instance)
    val writer = AccumuloLayerWriter(instance, dataTable, opts)
    // 构建GeoRDD
    // ProjectedExtent类型值必须赋予，否则创建元数据信息的时候会报找不到
    val geoRDD: RDD[(ProjectedExtent, Tile)] = sparkContext.hadoopGeoTiffRDD(hadoopInPath)

    // 创建元数据信息
    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(geoRDD, FloatingLayoutScheme(256))

    // 创建切片RDD
    val tiled: RDD[(SpatialKey, Tile)] = geoRDD.
      tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)

    // 设置投影和瓦片大小
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator, 512)

    val (maxZoom, reprojected) = TileLayerRDD(tiled, rasterMetaData)
      .reproject(WebMercator, layoutScheme, Bilinear)

    Pyramid.upLevels(reprojected, layoutScheme, 18, 10) { (rdd, z) =>
      val layerId = LayerId("layer_"+dataTable, z)
      if (attributeStore.layerExists(layerId)) {
        AccumuloLayerDeleter(attributeStore).delete(layerId)
      }
      // 这里我们选择的是索引方式，希尔伯特和Z曲线两种方式选择
      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
    }
  }
}
