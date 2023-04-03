package com.spark.demo.read

import akka.http.scaladsl.server.Directives.{as, entity}
import com.spark.demo.AccumuloRasterTiles
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.{GeoTiffTileMethods, MultibandGeoTiff, SinglebandGeoTiff, Tags}
import geotrellis.raster.{DoubleConstantNoDataCellType, Raster, Tile}
import geotrellis.spark.io.LayerQuery
import geotrellis.spark.{KeyBounds, LayerId, Metadata, SpatialKey, TileLayerMetadata, TileLayerRDD}
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
    val bounds = metadata.layout.mapTransform.extentFor(metadata.bounds)

    val extent = Extent(0, 0, 0, 0)
    val query = LayerQuery[SpatialKey].where(Intersects(extent))

    val rdd: RDD[(SpatialKey, Tile)] = reader.query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId, query)
      .mapValues(_.tile)

    val tileLayerRDD: TileLayerRDD[SpatialKey] = ContextRDD(rdd, metadata)

    //    将栅格数据保存为 Tiff 文件
    val raster: Raster[MultibandTile] = tileLayerRDD.stitch().crop(extent).raster
    val tiff = GeoTiff(raster)
    tiff.write("output.tiff")
    //    val tilesRdd = collectionLayerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
//    val rows = metadata.bounds.
    println(metadata.toString+"\n"+cols.minKey+"\n"+cols.maxKey)
  }
}