package com.spark.demo.read

import akka.http.scaladsl.server.Directives.{as, entity}
import com.spark.demo.AccumuloRasterTiles
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffTileMethods, MultibandGeoTiff, SinglebandGeoTiff, Tags}
import geotrellis.raster.{DoubleConstantNoDataCellType, MultibandTile, Raster, Tile}
import geotrellis.spark.io.LayerQuery
import geotrellis.spark.{ContextRDD, KeyBounds, LayerId, Metadata, SpatialKey, TileLayerMetadata, TileLayerRDD}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloCollectionLayerReader, AccumuloInstance, AccumuloLayerReader, AccumuloValueReader}
import geotrellis.spark.io.{Intersects, Reader, SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.vector.{Extent, Polygon}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
object Test {
  def main(args: Array[String]): Unit = {
    // 创建 Accumulo 数据库连接
    implicit val instance = AccumuloInstance("accumulo", "node1:2181,node2:2181,node3:2181",
      "root", new PasswordToken("root"))

    implicit val attrStore = AccumuloAttributeStore(instance.connector)
    val collectionLayerReader = AccumuloCollectionLayerReader(attrStore)
    val layerId = LayerId("tableName", 0)
    val metadata = attrStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)

    val tilesRdd = collectionLayerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
    val rows = metadata.bounds
    println(metadata.toString)
  }
}