package com.spark.demo.index

import geotrellis.proj4.WebMercator
import geotrellis.raster.Tile
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, Tags}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.{SpaceTimeKeyFormat, SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloCollectionLayerReader, AccumuloInstance, AccumuloLayerWriter, AccumuloValueReader}
import geotrellis.spark.{KeyBounds, LayerId, Metadata, SpaceTimeKey, SpatialKey, TemporalProjectedExtent, TileLayerMetadata, TileLayerRDD, withCollectMetadataMethods, withTilerMethods}
import geotrellis.spark.io.hadoop.{HadoopGeoTiffReader, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, Tiler, ZoomedLayoutScheme}
import geotrellis.vector.ProjectedExtent
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.commons.httpclient.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SelfIndex {

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

    val (18, reprojected): (Int, RDD[(SpatialKey, Tile)] with
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
    // val attributeStore = AccumuloAttributeStore(accumuloInstance, tablename)
    // val valueReader = AccumuloValueReader(accumuloInstance, attributeStore, LayerId("layerId", 18))
    val writer = AccumuloLayerWriter(accumuloInstance, tableName)

    //创建金字塔并进行切片，保存至accumulo
    Pyramid.upLevels(reprojected, tarlayoutScheme,startZoom, endZoom, Bilinear) { (rdd, z) =>
      val layerId = LayerId("layer_"+tableName, z)
//      val indexKeyBounds: KeyBounds[SpatialKey] = {
//        val KeyBounds(minKey, maxKey) = rdd.metadata.bounds.get // assuming non-empty layer
//              KeyBounds(
//                minKey.formatted(""),
//                maxKey.formatted("")
//              )
//        KeyBounds(minKey, maxKey)
//      }
//      val keyIndex =
//        ZCurveKeyIndexMethod.createIndex(indexKeyBounds)
      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
    }
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    makeslice(args(0),args(1).toInt,args(2).toInt,args(3))
  }
}