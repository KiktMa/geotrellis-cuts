package com.spark.demo

import com.spark.demo.read.WebServer.sparkContext
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import geotrellis.raster.{ArrayTile, Raster, Tile}
import geotrellis.spark.io.AttributeStore.Fields
import geotrellis.spark.io.{Reader, SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloCollectionLayerReader, AccumuloInstance, AccumuloLayerReader, AccumuloValueReader}
import geotrellis.vector.{Extent, Polygon}
import geotrellis.vector.io.json.{GeoJson, JsonFeatureCollection}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.{SparkConf, SparkContext}


object Test {

//  implicit val conf: SparkConf = new SparkConf().setAppName("ReadTiff")
//  implicit val sparkContext = new SparkContext(conf)
  // 创建 Accumulo 数据库连接
  implicit val instance = AccumuloInstance("accumulo", "node1:2181,node2:2181,node3:2181",
    "root", new PasswordToken("root"))

  implicit val attrStore = AccumuloAttributeStore(instance.connector)
//  val collectionLayerReader = AccumuloCollectionLayerReader(attrStore)
//  implicit val layerReader: AccumuloLayerReader = AccumuloLayerReader(instance)
  def main(args: Array[String]): Unit = {
//    val layerId = LayerId("layer_" + args(0), args(1).toInt)
//    test(layerId,args(2).toDouble,args(3).toDouble,args(4).toDouble,args(5).toDouble)
//    sparkContext.stop()
    readSpatialKey("slope", 12)
  }
  
  def readRegion(layerId: LayerId):Unit = {
    
//    val metadata = attrStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
//
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
//
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
//
//        val tilesRdd = collectionLayerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
//        val rows = metadata.bounds
//    println(metadata.toString + "\n" + result.swap)
  }

  def test(layerId: LayerId,xmin:Double,ymin:Double,xmax:Double,ymax:Double): Unit = {
//    val valueReader: Reader[SpatialKey, Tile] = AccumuloValueReader(instance, attrStore, layerId)
//    val metadata = attrStore.readMetadata(layerId)
//    val tile:Tile = valueReader.read(metadata)
//    val rep = tile.reproject(new Extent(0.0, 0, 0, 0), CRS.fromEpsgCode(3857), CRS.fromEpsgCode(3857)).tile
    //    val maskedTile = {
//      val poly = maskz.parseGeoJson[Polygon]
//      val extent: Extent = attrStore.read[TileLayerMetadata[SpatialKey]](LayerId("LayerName", 18), Fields.metadata).mapTransform(SpatialKey(0,0))
//      tile.mask(extent, poly.geom)
//    }
//    val tileLayerRDD = layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
    //    tileLayerRDD.saveAsTextFile("")
//    val tileLayer: Raster[Tile] = tileLayerRDD.stitch().crop(Extent(xmin, ymin, xmax, ymax))

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
//    GeoTiff(tileLayer, tileLayerRDD.metadata.crs).write("/app/tif/test/test.tif")
  }

  def readSpatialKey(tableName: String, level: Int): Unit = {
    val layerId = LayerId("layer_"+tableName, level)
    val valueReader: Reader[SpatialKey, Tile] = AccumuloValueReader(instance,attrStore,layerId)
    val tile:Tile = valueReader.read(SpatialKey(3092, 1719))

//    println(tile)
    val metadata = attrStore.read[TileLayerMetadata[SpatialKey]](layerId, Fields.metadata)

    val geoTiff = SinglebandGeoTiff(tile, metadata.extent, metadata.crs)
    geoTiff.write("D:\\test_tif\\18level\\test.tif")
//    val spatialKey = attrStore.read(layerId, "metadata")
    val cols = metadata.bounds.get

    //    val rows = metadata.bounds.
    println(metadata.toString + "\n" + cols.minKey + "\n" + cols.maxKey)
  }
}