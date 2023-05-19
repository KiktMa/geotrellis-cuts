package com.spark.demo

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import geotrellis.raster.{ArrayTile, DoubleConstantNoDataCellType, Raster, Tile}
import geotrellis.spark.io.AttributeStore.Fields
import geotrellis.spark.io.{Intersects, LayerQuery, Reader, SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.{Bounds, LayerId, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloCollectionLayerReader, AccumuloInstance, AccumuloLayerReader, AccumuloValueReader}
import geotrellis.spark.io.index.zcurve.Z2
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.{Extent, Point, Polygon}
import geotrellis.vector.io.json.{GeoJson, JsonFeatureCollection}
import geotrellis.vector.io.wkt.WKT
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
  implicit val layerReader: AccumuloLayerReader = AccumuloLayerReader(attrStore)
  def main(args: Array[String]): Unit = {
    val layerId = LayerId("layer_" + args(0), args(1).toInt)
    test(layerId,args(2).toDouble,args(3).toDouble,args(4).toDouble,args(5).toDouble,args(6))
    sparkContext.stop()
//  readSpatialKey("test1", 18)
  // Z曲线编码将二维空间键
//  val z: Z2 = Z2(197888,110139)
//  val byte = z.bitsToString
//  println(z+"\n"+byte)

//    readRegion(LayerId("layer_slope",18))
}
  
  def readRegion(layerId: LayerId):Unit = {

    // 获取某个坐标点对应的值
//    val key = attrStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
//      .mapTransform(Point(91.75386428833008,27.804499561428514))
//    val (col, row) = attrStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
//      .toRasterExtent().mapToGrid(Point(91.75386428833008,27.804499561428514))
//    val tile: Tile = layerReader.reader[SpatialKey, Tile, Bounds[SpatialKey]](layerId).read(key)
//    val tileCol = col - key.col * tile.cols
//    val tileRow = row - key.row * tile.rows
//    println(s"tileCol=${tileCol}   tileRow = ${tileRow}")
//    tile.get(tileCol, tileRow)
    // 求瓦片坡度
//    tile.slope(getMetaData(LayerId("LayerName", 0)).layout.cellSize, 1.0, None)
//
    val maskz: String = "{\n" +
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

//    val polygon = WKT.read("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))").asInstanceOf[Polygon]
//    val query = new LayerQuery[SpatialKey, TileLayerMetadata[SpatialKey]]().where(Intersects(polygon))

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
//      val tilesRdd = collectionLayerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
//      val rows = metadata.bounds
//    println(metadata.toString + "\n" + result.swap)
  }

  def test(layerId: LayerId,xmin:Double,ymin:Double,xmax:Double,ymax:Double,fileName:String): Unit = {
//    val valueReader: Reader[SpatialKey, Tile] = AccumuloValueReader(instance, attrStore, layerId)
//    val metadata = attrStore.readMetadata(layerId)
//    val tile:Tile = valueReader.read(metadata)
//    val rep = tile.reproject(new Extent(0.0, 0, 0, 0), CRS.fromEpsgCode(3857), CRS.fromEpsgCode(3857)).tile
//        val maskedTile = {
//      val poly = maskz.parseGeoJson[Polygon]
//      val extent: Extent = attrStore.read[TileLayerMetadata[SpatialKey]](LayerId("LayerName", 18), Fields.metadata).mapTransform(SpatialKey(0,0))
//      tile.mask(extent, poly.geom)
//    }
    val tileLayerRDD = layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
//        tileLayerRDD.saveAsTextFile("")
//    val tile:Raster[Tile] = tileLayerRDD
//      .mask(Polygon(Seq(Point(xmin, ymin), Point(xmax, ymin),Point(xmax,ymax),Point(xmin,ymax),Point(xmin, ymin))))
//      .crop(Extent(xmin, ymin, xmax, ymax))
//      .stitch()
    val tileLayer: Raster[Tile] = tileLayerRDD.stitch()

//        val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(LatLng, tileSize = 512)
//        val topLeft = Pyramid.levelStream(tileLayerRDD, layoutScheme, 18, 18).map(md => {
//          val layout = md._2.metadata.layout
//          val extent = md._2.metadata.extent
//          val rows = layout.tileRows
//          val cols = layout.tileCols
//          val tileLayout = layout.tileLayout
//          val bounds = md._2.metadata.bounds
//
//        })
//        topLeft.foreach(println)
    GeoTiff(tileLayer._1, Extent(xmin, ymin, xmax, ymax), tileLayerRDD.metadata.crs).write("/app/tif/test/"+ fileName +".tif")
  }

  def readSpatialKey(tableName: String, level: Int): Unit = {
    val layerId = LayerId("layer_"+tableName, level)
    val valueReader: Reader[SpatialKey, Tile] = AccumuloValueReader(instance,attrStore,layerId)
//    val tile:Tile = valueReader.read(SpatialKey(0, 0))

//    println(tile)
    val metadata = attrStore.read[TileLayerMetadata[SpatialKey]](layerId, Fields.metadata)
//    val extent = metadata.layout.mapTransform(SpatialKey(0, 0))
//    val layout = metadata.layout.tileLayout
//    val definition = LayoutDefinition(metadata.extent, metadata.tileLayout)
//    val layout1 = definition.tileLayout
    //    val geoTiff = SinglebandGeoTiff(tile, metadata.extent, metadata.crs)
//    geoTiff.write("D:\\test_tif\\18level\\test.tif")
//    val spatialKey = attrStore.read(layerId, "metadata")
    val cols = metadata.bounds.get
    val size = metadata.cellSize
    val cellType = metadata.cellType
    //    val rows = metadata.bounds.
    println(metadata.toString + "\n" + cols.minKey + "\n" + cols.maxKey+"\n"+size+"\n"+cellType)
  }
}