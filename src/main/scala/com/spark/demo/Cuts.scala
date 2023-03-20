package com.spark.demo

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.Boundable
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloInstance, AccumuloLayerReader, AccumuloLayerWriter}
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import geotrellis.spark.pyramid._
import geotrellis.spark.tiling._
import geotrellis.vector._
import monocle.PLens
import org.apache.accumulo.core.client.{ClientConfiguration, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.rdd.RDD

import java.io.File

object Cuts {
  //  输入文件路径
  private val hadoopInPath: String = "hdfs://node1:9000/tiff/caijian_zhuanhuan.tif"
  //  输出文件路径
  private val outFilePath: String = "/app/tif"

  //  spark Master
  private val master: String = "yarn"
  //  spark 应用名称
  private val appName: String = "GeoTrellis2Accumulo"

  //  切片等级
  private val setZoom: Int = 8
  private val tileSize: Int = 512

  //  是否生成PNG
  private val isPNG: Boolean = false
  //  实例名称
  private val instanceFileName: String = "caijian"
  //  图片实例名称
  private val InstancePNGFileName: String = "test"

  private val sparkConf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(master)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

  val sparkContext = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {
    try {
      run(sparkContext)
    } finally {
      sparkContext.stop()
    }
  }

  def run(implicit sparkContext: SparkContext): Unit = {

    val instanceName = "accumulo"
    val zookeepers = "node1:2181,node2:2181,node3:2181"
    val user = "root"
    val password = "root"
    val table = "tiles"
    //    构建GeoRDD
    //    ProjectedExtent类型值必须赋予，否则创建元数据信息的时候会报找不到
    val geoRDD: RDD[(ProjectedExtent, Tile)] = sparkContext.hadoopGeoTiffRDD(hadoopInPath)

    //    创建元数据信息
    val (_, rasterMetaData) = TileLayerMetadata.fromRDD(geoRDD, FloatingLayoutScheme(512))

    //    创建切片RDD
    val tiled: RDD[(SpatialKey, Tile)] = geoRDD.
      tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
    // 请特别注意，此处咨询过 GeoTrellis 的作者，请不要随意的使用repartition!!
    // repartition 会将所有的数据进行重新分区，增加计算量！
    //  .repartition(100)

    //    设置投影和瓦片大小
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = tileSize)

    val (maxZoom, reprojected) = TileLayerRDD(tiled, rasterMetaData)
      .reproject(WebMercator, layoutScheme, Bilinear)

    val metadata = reprojected.metadata

    //    创建输出存储区
//    val attributeStore = FileAttributeStore(outFilePath)
//    val writer = FileLayerWriter(attributeStore)

//    val colorRender = ColorRamps.LightToDarkSunset

    Pyramid.upLevels(reprojected, layoutScheme, setZoom, Bilinear) { (rdd, z) =>
      val writer = AccumuloLayerWriter(AccumuloInstance.apply(instanceName, zookeepers, user, new PasswordToken(password)), table)

      val layerId = LayerId("layer_name", z)

      // 这里我们选择的是索引方式，希尔伯特和Z曲线两种方式选择
      writer.write(layerId, rdd, HilbertKeyIndexMethod)
    }
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
