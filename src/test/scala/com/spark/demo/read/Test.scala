package com.spark.demo.read

import akka.http.scaladsl.server.Directives.{as, entity}
import com.geosot.javademo.AccumuloRasterTiles
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffTileMethods, MultibandGeoTiff, SinglebandGeoTiff, Tags}
import geotrellis.raster.{DoubleConstantNoDataCellType, MultibandTile, Raster, Tile}
import geotrellis.spark.io.AttributeStore.Fields
import geotrellis.spark.io.LayerQuery
import geotrellis.spark.{ContextRDD, KeyBounds, LayerId, Metadata, SpatialKey, TileLayerMetadata, TileLayerRDD}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloCollectionLayerReader, AccumuloInstance, AccumuloLayerReader, AccumuloValueReader}
import geotrellis.spark.io.{Intersects, Reader, SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.vector.{Extent, Polygon}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import geotrellis.vector.io.json._
object Test {
  def main(args: Array[String]): Unit = {
    // 创建 Accumulo 数据库连接
    implicit val instance = AccumuloInstance("accumulo", "node1:2181,node2:2181,node3:2181",
      "root", new PasswordToken("root"))

    implicit val attrStore = AccumuloAttributeStore(instance.connector)
    val collectionLayerReader = AccumuloCollectionLayerReader(attrStore)
    val layerId = LayerId("tableName", 0)
    val metadata = attrStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)

    val maskz:String = "{\n" +
      "      \"type\": \"FeatureCollection\"\n" +
      "      ,\n" +
      "      \"features\":\n" +
      "      [\n" +
      "      {\n" +
      "        \"type\": \"Feature\"\n" +
      "        ,\n" +
      "        \"properties\": {}\n" +
      "        ,\n" +
      "        \"geometry\": {\n" +
      "          \"type\": \"Polygon\",\n" +
      "          \"coordinates\": [\n" +
      "          [\n" +
      "          [\n" +
      "          91.75386428833008,\n" +
      "          27.804499561428514\n" +
      "          ],\n" +
      "          [\n" +
      "          91.76901340484618,\n" +
      "          27.804499561428514\n" +
      "          ],\n" +
      "          [\n" +
      "          91.76901340484618,\n" +
      "          27.817139680870127\n" +
      "          ],\n" +
      "          [\n" +
      "          91.75386428833008,\n" +
      "          27.817139680870127\n" +
      "          ],\n" +
      "          [\n" +
      "          91.75386428833008,\n" +
      "          27.804499561428514\n" +
      "          ]\n" +
      "          ]\n" +
      "          ]\n" +
      "        }\n" +
      "      }\n" +
      "      ]\n" +
      "    }"

    val geoJson = GeoJson.parse(maskz)
    val features = geoJson.asInstanceOf[JsonFeatureCollection].getAllPolygons()
    val poly = features.headOption.getOrElse(throw new Exception("No polygon found"))

    val layerMetadata = attrStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
    val queryPoly = poly.reproject(LatLng, layerMetadata.crs)

    val fn: Tile => Tile = {
      tile: Tile => tile.convert(DoubleConstantNoDataCellType)
    }

    // Query all tiles that intersect the polygon and build histogram
    val queryHist = collectionLayerReader
      .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
      .where(Intersects(queryPoly))
      .result // all intersecting tiles have been fetched at this point
      .withContext(_.mapValues(fn))
      .polygonalHistogramDouble(queryPoly)

    val result: (Double, Double) =
      queryHist.minMaxValues().getOrElse((Double.NaN, Double.NaN))

//    val tilesRdd = collectionLayerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
//    val rows = metadata.bounds
    println(metadata.toString+"\n"+result.swap)
  }
}