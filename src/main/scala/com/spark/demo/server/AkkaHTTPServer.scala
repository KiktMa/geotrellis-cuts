//package com.spark.demo.server
//
//import akka.actor.ActorSystem
//import akka.http.scaladsl.Http
//import akka.http.scaladsl.server.Directives._
//import akka.stream.ActorMaterializer
//import geotrellis.raster.Tile
//import geotrellis.spark._
//import geotrellis.spark.io._
//import geotrellis.spark.io.accumulo._
//import geotrellis.spark.tiling.ZoomedLayoutScheme
//import geotrellis.vector.Extent
//import geotrellis.vector.io.ExtentsToGeoJson
//import org.apache.accumulo.core.client.security.tokens.PasswordToken
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.concurrent.ExecutionContextExecutor
//import scala.util.{Failure, Success}
//
//object AkkaHTTPServer {
//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf().setAppName("ReadLayer")
//    implicit val sparkContext = new SparkContext(conf)
//    implicit val system: ActorSystem = ActorSystem("my-system")
//    implicit val materializer: ActorMaterializer = ActorMaterializer()
//    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
//
//    // 创建Accumulo连接和数据读取器
//    val instance = AccumuloInstance("accumulo", "node1:2181,node2:2181,node3:2181", "root",
//      new PasswordToken("root"))
//    val attributeStore = AccumuloAttributeStore(instance)
//    val readLayer = AccumuloLayerReader(instance)
//
//    // 创建ZoomedLayoutScheme瓦片布局方案
////    val layoutScheme = ZoomedLayoutScheme()
//
//    // 定义瓦片服务路由
//    val route =
//      path("wmts" / Segment / IntNumber / IntNumber / IntNumber) { (layerName, zoom, x, y) =>
//        complete {
//          // 从Accumulo数据库中读取指定层级的瓦片数据
//          val layerId = LayerId(layerName, zoom)
//          val metadata = attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
//          val tileExtent: Extent = metadata.mapTransform(SpatialKey(x, y)).extent
//          val tile = readLayer.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
//
//          // 生成WMTS瓦片请求URL
//          val tileScheme = new GlobalTileScheme()
//          val tileBounds = metadata.bounds
////          val tileLayout = layoutScheme.levelForZoom(zoom).layout
//          val (col, row) = tileScheme.pointToTile(tileExtent.xmin, tileExtent.ymin, zoom)
//          val wmtsUrl = s"http://localhost:8080/geoserver/gwc/service/wmts?layer=${layerName}&style=&tilematrixset=EPSG:900913&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image%2Fpng&TileMatrix=${zoom}&TileCol=${col}&TileRow=${row}"
//
//          // 返回WMTS瓦片请求URL
//          wmtsUrl
//        }
//      }
//
//    // 启动Akka HTTP服务
//    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
//    bindingFuture.onComplete {
//      case Success(binding) =>
//        val localAddress = binding.localAddress
//        println(s"Server online at http://${localAddress.getHostString}:${localAddress.getPort}/")
//      case Failure(ex) =>
//        println(s"Server failed to start: ${ex.getMessage}")
//    }
//  }
//}
//
