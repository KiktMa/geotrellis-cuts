package com.spark.demo.index

import geotrellis.proj4.WebMercator
import geotrellis.raster.{CellType, Tile}
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.{LayerType, SpaceTimeKeyFormat, SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloCollectionLayerReader, AccumuloInstance, AccumuloLayerDeleter, AccumuloLayerWriter, AccumuloValueReader}
import geotrellis.spark.io.file.FileLayerHeader
import geotrellis.spark.{KeyBounds, LayerId, Metadata, SpaceTimeKey, SpatialKey, TemporalProjectedExtent, TileLayerMetadata, TileLayerRDD, withCollectMetadataMethods, withTilerMethods}
import geotrellis.spark.io.hadoop.{HadoopGeoTiffReader, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.ContextRDD
import geotrellis.spark.tiling.{FloatingLayoutScheme, Tiler, ZoomedLayoutScheme}
import geotrellis.vector.ProjectedExtent
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
object SelfIndex {

  /**
   * 这里需要在提交spark任务时传入四个参数到main中的args()中
   * @param inputPath 栅格数据在hdfs中的存储位置
   * @param startZoom 金字塔模型的开始层，也即最底层
   * @param endZoom 金字塔模型的结束层，也即最顶层
   * @param tableName 金字塔瓦片存储的表名，需事先在accumulo数据库中创建表否则会在每个节点中自动创建很多表
   */
  def makeslice(inputPath: String,startZoom:Int,endZoom:Int,tableName:String) = {
    val conf =
      new SparkConf()
        .setMaster("yarn")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    runtoAccumuloTime(sc, inputPath,startZoom,endZoom,tableName)
  }

  def runtoAccumuloTime(implicit sc: SparkContext, inputPath: String,startZoom:Int,endZoom:Int,tableName:String) = {

//    val hdfsConf = new Configuration()
//    val hdfs = new Path("hdfs://node1:9000")
//    //将时间添加至tif文件的head信息中，并生成新的tif
//    val newTiffPath = new Path(hdfs,"/tiff/t0.tif")
//
//    val time = "2023:03:29 00:00:00"
//    val geoTiff: SinglebandGeoTiff = HadoopGeoTiffReader.readSingleband(inputPath)
//    val t = Map(Tags.TIFFTAG_DATETIME -> time)
//    val newtiff = new SinglebandGeoTiff(geoTiff.tile, geoTiff.extent, WebMercator, Tags(t, geoTiff.tags.bandTags), geoTiff.options)
//
//    val outputStream = hdfs.getFileSystem(hdfsConf).create(newTiffPath)
//    GeoTiffWriter.write(newtiff,outputStream)
//    outputStream.close()

    //生成rdd
    val inputRdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(inputPath)

    //创建元数据信息
    val layoutScheme = FloatingLayoutScheme(512)
    val (_, rasterMetaData: TileLayerMetadata[SpatialKey]) =
      inputRdd.collectMetadata[SpatialKey](layoutScheme)

    val tilerOptions =
      Tiler.Options(
        resampleMethod = Bilinear,
        partitioner = Some(new HashPartitioner(inputRdd.partitions.length))
      )

    //生成SpaceTimeKey RDD
    val tiledRdd: RDD[(SpatialKey, Tile)] with
      Metadata[TileLayerMetadata[SpatialKey]] =
      inputRdd.tileToLayout[SpatialKey](rasterMetaData, tilerOptions)

    //切片大小
    val tarlayoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 512)

    val (_, reprojected): (Int, RDD[(SpatialKey, Tile)] with
      Metadata[TileLayerMetadata[SpatialKey]]) =
      TileLayerRDD(tiledRdd, rasterMetaData)
        .reproject(WebMercator, tarlayoutScheme, Bilinear)

    //accumulo的配置内容，这里示例如下
    val instance = "accumulo"
    val zookeepers = "node1:2181,node2:2181,node3:2181"
    val user = "root"
    val password = "root"

    //创建accumulo存储对象
    val accumuloInstance: AccumuloInstance = AccumuloInstance(instance, zookeepers, user, new PasswordToken(password))
    val attributeStore = AccumuloAttributeStore(accumuloInstance)
    // val valueReader = AccumuloValueReader(accumuloInstance, attributeStore, LayerId("layerId", 18))
    val writer = AccumuloLayerWriter(accumuloInstance, tableName)

    //创建金字塔并进行切片，保存至accumulo
    Pyramid.upLevels(reprojected, tarlayoutScheme,startZoom, endZoom, Bilinear) { (rdd, z) =>
      val layerId = LayerId("layer_"+tableName, z)

//      val tilesWithMetadata: RDD[(SpatialKey, Tile, TileLayerMetadata[String])] = rdd.map { case (key, tile) =>
//        val topLeftCode = GeoSot.INSTANCE.getHexCode(rdd.metadata.extent.xmin, rdd.metadata.extent.ymax, 0, z)
//        val bottomRightCode = GeoSot.INSTANCE.getHexCode(rdd.metadata.extent.xmax, rdd.metadata.extent.ymin, 0, z)
//        val metaData = TileLayerMetadata(
//          cellType = tile.cellType,
//          layout = rdd.metadata.layout,
//          extent = rdd.metadata.mapTransform(key),
//          crs = rdd.metadata.crs,
//          bounds = KeyBounds(topLeftCode, bottomRightCode)
//        )
//        (key, tile, metaData)
//      }
//      val indexKeyBounds: KeyBounds[SpatialKey] = {
//        val KeyBounds(minKey, maxKey):KeyBounds[SpatialKey] = tilesWithMetadata.foreach((S) => S._1)
//        KeyBounds(minKey, maxKey)
//      }
//      val keyIndex =
//        ZCurveKeyIndexMethod.createIndex(indexKeyBounds)

      if (attributeStore.layerExists(layerId)) {
        AccumuloLayerDeleter(attributeStore).delete(layerId)
      }
      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
    }
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    makeslice(args(0),args(1).toInt,args(2).toInt,args(3))
  }
}