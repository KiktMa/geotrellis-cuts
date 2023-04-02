package com.spark.demo.read

import geotrellis.raster.{Raster, Tile}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.io.{SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloInstance, AccumuloLayerReader}
import geotrellis.vector.Extent
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Read").setMaster("yarn")
    implicit val sparkContext: SparkContext = new SparkContext(sparkConf)
    // 创建 Accumulo 数据库连接
    implicit val instance: AccumuloInstance = AccumuloInstance("accumulo", "node1:2181,node2:2181,node3:2181",
      "root", new PasswordToken("root"))
    val attributeStore = AccumuloAttributeStore(instance.connector)
    val reader = AccumuloLayerReader(attributeStore)
    val layerId: LayerId = LayerId("layer-", 10) // 图层名称和缩放级别

    // 读取指定图层和缩放级别的数据
    val rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
    val masked = rdd.mask(Extent(0, 0, 0, 0))
    val stitch = masked.stitch()
    val tile = stitch.tile

    val raster: Raster[Tile] = Raster(tile, Extent(0, 0, 0, 0))
  }
}
