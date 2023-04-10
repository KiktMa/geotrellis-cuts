package com.spark.demo

import com.spark.demo.read.WebServer.sparkContext
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{Raster, Tile}
import geotrellis.spark.io.{SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloCollectionLayerReader, AccumuloInstance, AccumuloLayerReader}
import geotrellis.vector.Extent
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.{SparkConf, SparkContext}


object Test {

  implicit val conf: SparkConf = new SparkConf().setAppName("ReadTiff")
  implicit val sparkContext = new SparkContext(conf)
  // 创建 Accumulo 数据库连接
  implicit val instance = AccumuloInstance("accumulo", "node1:2181,node2:2181,node3:2181",
    "root", new PasswordToken("root"))

  implicit val attrStore = AccumuloAttributeStore(instance.connector)
  val collectionLayerReader = AccumuloCollectionLayerReader(attrStore)
  implicit val layerReader: AccumuloLayerReader = AccumuloLayerReader(instance)
  def main(args: Array[String]): Unit = {
    val layerId = LayerId("layer_" + args(0), args(1).toInt)
    test(layerId)
    sparkContext.stop()
  }
  
  def readRegion(layerId: LayerId):Unit = {
    
//    val metadata = attrStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)

//    val maskz: String = "{\n" +
//      "      \"type\": \"FeatureCollection\"\n" +
//      "      ,\n" +
//      "      \"features\":\n" +
//      "      [\n" +
//      "      {\n" +
//      "        \"type\": \"Feature\"\n" +
//      "        ,\n" +
//      "        \"properties\": {}\n" +
//      "        ,\n" +
//      "        \"geometry\": {\n" +
//      "          \"type\": \"Polygon\",\n" +
//      "          \"coordinates\": [\n" +
//      "          [\n" +
//      "          [\n" +
//      "          91.75386428833008,\n" +
//      "          27.804499561428514\n" +
//      "          ],\n" +
//      "          [\n" +
//      "          91.76901340484618,\n" +
//      "          27.804499561428514\n" +
//      "          ],\n" +
//      "          [\n" +
//      "          91.76901340484618,\n" +
//      "          27.817139680870127\n" +
//      "          ],\n" +
//      "          [\n" +
//      "          91.75386428833008,\n" +
//      "          27.817139680870127\n" +
//      "          ],\n" +
//      "          [\n" +
//      "          91.75386428833008,\n" +
//      "          27.804499561428514\n" +
//      "          ]\n" +
//      "          ]\n" +
//      "          ]\n" +
//      "        }\n" +
//      "      }\n" +
//      "      ]\n" +
//      "    }"

//    val geoJson = GeoJson.parse(maskz)
//    val features = geoJson.asInstanceOf[JsonFeatureCollection].getAllPolygons()
//    val poly = features.headOption.getOrElse(throw new Exception("No polygon found"))
//
//    val layerMetadata = attrStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
//    val queryPoly = poly.reproject(LatLng, layerMetadata.crs)
//
//    val fn: Tile => Tile = {
//      tile: Tile => tile.convert(DoubleConstantNoDataCellType)
//    }
//
//    // Query all tiles that intersect the polygon and build histogram
//    val queryHist = collectionLayerReader
//      .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
//      .where(Intersects(queryPoly))
//      .result // all intersecting tiles have been fetched at this point
//      .withContext(_.mapValues(fn))
//      .polygonalHistogramDouble(queryPoly)
//
//    val result: (Double, Double) =
//      queryHist.minMaxValues().getOrElse((Double.NaN, Double.NaN))

    //    val tilesRdd = collectionLayerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
    //    val rows = metadata.bounds
//    println(metadata.toString + "\n" + result.swap)
  }

  def test(layerId: LayerId): Unit = {
    //    val valueReader: Reader[SpatialKey, Tile] = AccumuloValueReader(instance, attrStore, layerId)
    //    val metadata = attrStore.readMetadata(layerId)
    //    val tile:Tile = valueReader.read(metadata)
    //    val maskedTile = {
    //      val poly = maskz.parseGeoJson[Polygon]
    //      val extent: Extent = attrStore.read[TileLayerMetadata[SpatialKey]](LayerId("LayerName", 18), Fields.metadata).mapTransform(SpatialKey(0,0))
    //      tile.mask(extent, poly.geom)
    //    }
    val tileLayerRDD = layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
    //    tileLayerRDD.saveAsTextFile("")
    val tileLayer: Raster[Tile] = tileLayerRDD.stitch()
    //    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(LatLng, tileSize = 512)
    //    val topLeft = Pyramid.levelStream(tileLayerRDD, layoutScheme, 18, 18).map(md => {
    //      val layout = md._2.metadata.layout
    //      val extent = md._2.metadata.extent
    //      val rows = layout.tileRows
    //      val cols = layout.tileCols
    //      val tileLayout = layout.tileLayout
    //      val bounds = md._2.metadata.bounds
    //
    //    })
    //    topLeft.foreach(println)
    GeoTiff(tileLayer._1, Extent(10237450.287, 3196984.713, 10239779.287, 3199173.213), CRS.fromEpsgCode(3857)).write("/app/tif/test.tif")
  }
}