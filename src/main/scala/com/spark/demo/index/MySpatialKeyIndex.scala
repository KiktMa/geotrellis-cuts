package com.spark.demo.index

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.Tile
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.{SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloInstance, AccumuloLayerDeleter, AccumuloLayerWriter}
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata, TileLayerRDD, withTilerMethods}
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.vector.{Point, ProjectedExtent}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object MySpatialKeyIndex{
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("appName")
      .setMaster("yarn")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    implicit val sparkContext: SparkContext = new SparkContext(sparkConf)

    val instanceName = "accumulo"
    val zookeepers = "node1:2181,node2:2181,node3:2181"
    val user = "root"
    val password = "root"
    //    构建GeoRDD
    //    ProjectedExtent类型值必须赋予，否则创建元数据信息的时候会报找不到
    val geoRDD: RDD[(ProjectedExtent, Tile)] = sparkContext.hadoopGeoTiffRDD("path")

    //    创建元数据信息
    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(geoRDD, FloatingLayoutScheme(512))

    //    创建切片RDD
    val tiled: RDD[(SpatialKey, Tile)] = geoRDD.
      tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
      .repartition(50)

    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(LatLng, tileSize = 512)

    val (_, reprojected) = TileLayerRDD(tiled, rasterMetaData)
      .reproject(WebMercator, layoutScheme, Bilinear)

    val instance = AccumuloInstance(instanceName, zookeepers, user, new PasswordToken(password))
    val attributeStore = AccumuloAttributeStore(instance)
    val store = AccumuloLayerWriter(instance, attributeStore, "tableName")

    Pyramid.upLevels(reprojected, layoutScheme, 18, 18) { (rdd, z) =>

      val layerId = LayerId("layer_tableName", z)
      if (attributeStore.layerExists(layerId)) {
        AccumuloLayerDeleter(attributeStore).delete(layerId)
        // 这里我们选择的是索引方式，希尔伯特和Z曲线两种方式选择
        store.write(layerId, rdd, ZCurveKeyIndexMethod)
      }
      sparkContext.stop()
    }
  }
}
