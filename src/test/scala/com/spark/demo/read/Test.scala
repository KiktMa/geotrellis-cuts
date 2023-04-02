package com.spark.demo.read

import geotrellis.raster.Tile
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloInstance, AccumuloLayerReader, AccumuloValueReader}
import geotrellis.spark.io.{Reader, SpatialKeyFormat, tileLayerMetadataFormat}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    // 创建 Accumulo 数据库连接
    val instance = AccumuloInstance("accumulo", "node1:2181,node2:2181,node3:2181",
      "root", new PasswordToken("root"))

    implicit val attrStore = AccumuloAttributeStore(instance.connector)

    val layerId = LayerId("layer_test", 10)

//    val valueReader: Reader[SpatialKey, Tile] = AccumuloValueReader(instance,attrStore,layerId)
//    val tile = valueReader.read(SpatialKey(1, 1))
//    println(tile)
    val metadata = attrStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
    val cols = metadata.bounds.get

//    val rows = metadata.bounds.
    println(metadata.toString+"\n"+cols.minKey+"\n"+cols.maxKey)
  }
}
