package com.spark.demo

//import com.spark.demo.geosot.GeoSot

//import com.spark.demo.index.GeoSot
import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.operation.valid.IsValidOp
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.Boundable
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloInstance, AccumuloLayerDeleter, AccumuloLayerReader, AccumuloLayerWriter}
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hbase.{HBaseAttributeStore, HBaseCollectionLayerReader, HBaseInstance, HBaseLayerDeleter, HBaseLayerWriter, HBaseValueReader}
import geotrellis.spark.io.index._
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import geotrellis.spark.pyramid._
import geotrellis.spark.tiling._
import geotrellis.vector._
import monocle.PLens
import org.apache.accumulo.core.client.{ClientConfiguration, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.commons.httpclient.URI
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.rdd.RDD

import java.io.File

object Cuts {
  // 输入文件路径
  private val hadoopInPath: String = "hdfs://node1:9000/tiff/road_slop_8bit.tif"
  // 输出文件路径
  private val outFilePath: String = "/app/tif"

  // spark Master
  private val master: String = "yarn"
  // spark 应用名称
  private val appName: String = "GeoTrellis2Accumulo"

  // 切片等级
  private val setZoom: Int = 18
  private val tileSize: Int = 512

  // 是否生成PNG
  private val isPNG: Boolean = false
  // 实例名称
  private val instanceFileName: String = "caijian"
  // 图片实例名称
  private val InstancePNGFileName: String = "test"

  private val sparkConf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(master)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

  val sparkContext = new SparkContext(sparkConf)

  /**
   * @param args args(0) tableName
   *             args(1) path
   *             args(2) part
   *             args(3) startZoom
   *             args(4) endZoom
   */
  def main(args: Array[String]): Unit = {
    try {
      run(sparkContext,args(0),args(1),args(2),args(3),args(4),args(5).toInt)
    } finally {
      sparkContext.stop()
    }
  }

  def run(implicit sparkContext: SparkContext,tableName:String,
          path:String,part:String,startZoom:String,endZoom:String,tileSize:Int): Unit = {

    val instanceName = "accumulo"
    val zookeepers = "node1:2181,node2:2181,node3:2181"
    val user = "root"
    val password = "root"
    // 构建GeoRDD
    // ProjectedExtent类型值必须赋予，否则创建元数据信息的时候会报找不到
    val geoRDD: RDD[(ProjectedExtent, Tile)] = sparkContext.hadoopGeoTiffRDD(path)

    // 创建元数据信息
    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(geoRDD, FloatingLayoutScheme(tileSize))

//    val tileCols = rasterMetaData.tileLayout.layoutCols
//    val tileRows = rasterMetaData.tileLayout.layoutRows
//    val tileWidth = rasterMetaData.tileLayout.tileCols
//    val tileHeight = rasterMetaData.tileLayout.tileRows
//    val layoutDefinition = LayoutDefinition(rasterMetaData.extent,
//      TileLayout(tileCols, tileRows, tileWidth, tileHeight))

    // 创建切片RDD
    val tiled: RDD[(SpatialKey, Tile)] = geoRDD
      .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout)
    // 请特别注意，不要随意的使用repartition!
    // repartition 会将所有的数据进行重新分区，增加计算量！
     .repartition(part.toInt)

    // 设置投影和瓦片大小
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator, tileSize)
//    val layoutScheme: FloatingLayoutScheme = FloatingLayoutScheme(tileSize)
//    val gridExtent = new GridExtent(Extent(1.0214554023750909E7, 3179116.727955985, 1.0246290023750909E7, 3180229.227955985), CellSize(0.5,0.5))
//    val layoutDefinition = LayoutDefinition(gridExtent, tileSize = 512)
    val (_, reprojected) = TileLayerRDD(tiled, rasterMetaData)
      .reproject(WebMercator, layoutScheme)

    // 创建输出存储区
    // val attributeStore = FileAttributeStore(outFilePath)
    // val writer = FileLayerWriter(attributeStore)
    // val colorRender = ColorRamps.LightToDarkSunset

    // 将金字塔模型存储到hbase数据库中
    val hinstance = HBaseInstance(Seq("node1","node2","node3"),"")
    val hBaseAttributeStore = HBaseAttributeStore(hinstance)
    val hwriter = HBaseLayerWriter(hinstance, tableName)

    // 将金字塔模型存储在accumulo数据库中
//    val instance = AccumuloInstance(instanceName, zookeepers, user, new PasswordToken(password))
//    val attributeStore = AccumuloAttributeStore(instance)
//    val writer = AccumuloLayerWriter(instance, attributeStore,tableName)

    // 将金字塔模型存储在hdfs分布式文件系统中
//    val catalogPathHdfs = new Path("hdfs://node1:9000/tiff/tile")
//    val attributeStore = HadoopAttributeStore(catalogPathHdfs)      // 创建HDFS写入对象
//    val writer = HadoopLayerWriter(catalogPathHdfs, attributeStore)

    Pyramid.upLevels(reprojected, layoutScheme, startZoom.toInt, endZoom.toInt, Bilinear) { (rdd, z) =>
      val layerId = LayerId("layer_"+tableName, z)
      // 判断hbase中是否已经存在该金字塔模型
      if(hBaseAttributeStore.layerExists(layerId)){
        HBaseLayerDeleter(hBaseAttributeStore).delete(layerId)
      }
//      if(attributeStore.layerExists(layerId)){
//        AccumuloLayerDeleter(attributeStore).delete(layerId)
//      }
      // 判断hdfs中是否已经存在该金字塔模型
//      if (attributeStore.layerExists(layerId)) {
//        HadoopLayerDeleter(attributeStore).delete(layerId)
//      }
      // 这里我们选择的是索引方式，希尔伯特和Z曲线两种方式选择
      hwriter.write(layerId, rdd, ZCurveKeyIndexMethod)
    }
    sparkContext.stop()
  }

  def renderPNG(implicit sparkContext: SparkContext, zoom: Int, colorRender: Any): Unit = {
    val path = outFilePath + "/" + instanceFileName
    val file = new File(path)

    //    如果不存在，则直接停止运行
    if (!file.exists()) {
      sparkContext.stop()
    }

    //    创建png结果文件夹
    val pngOutFileName = outFilePath + "/" + InstancePNGFileName
    createFolder(pngOutFileName)

    //    获取文件名
    val files = file.listFiles()

    files.foreach(item => {
      val zoomLevel = item.getName

      //    创建级别文件夹
      val zoomLevelFileName = pngOutFileName + "/" + zoomLevel
      createFolder(zoomLevelFileName)

      //    读取图层
      val reader = FileLayerReader(outFilePath)
      val layerId = LayerId(instanceFileName, zoomLevel.toInt)

      /*
      * 应该导入 import geotrellis.spark.io._
      * */
      val layers: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
        reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)

      //      GeoTrellis 与Google 的 ZXY相同
      layers.foreach(layer => {
        val key = layer._1
        val tile = layer._2

        //        定义x
        val outPathPNGYFileName = zoomLevelFileName + '/' + key.col
        createFolder(outPathPNGYFileName)

        //        定义y
        val pngFileName = outPathPNGYFileName + "/" + key.row + ".png"

        colorRender match {
          case ramp: ColorRamp =>
            tile.renderPng(ramp).write(pngFileName)

          case ramp: ColorMap =>
            tile.renderPng(ramp).write(pngFileName)
        }
      })
    })
  }

  /*
* 创建文件夹
* */
  def createFolder(folderName: String): File = {
    val file = new File(folderName)

    //    如果没有图片输出文件夹，则创建
    if (!file.exists()) {
      file.mkdirs()
    }
    file
  }
}