package com.spark.demo.read

import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.io.{ValueReader, _}
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import akka.actor._
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import akka.stream.{ActorMaterializer, Materializer}
import spray.json.DefaultJsonProtocol._

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloCollectionLayerReader, AccumuloInstance, AccumuloValueReader}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.{SparkConf, SparkContext}

object WebServer {
  def main(args: Array[String]): Unit = {

    val instanceName = "accumulo"
    val zookeepers = "node1:2181,node2:2181,node3:2181"
    val user = "root"
    val password = "root"

    val sparkConf = new SparkConf().setAppName("Server").setMaster("yarn")
    implicit val sparkContext: SparkContext = new SparkContext(sparkConf)
    // 创建 Accumulo 数据库连接
    implicit val instance: AccumuloInstance = AccumuloInstance("accumulo", "node1:2181,node2:2181,node3:2181",
      "root", new PasswordToken("root"))
    implicit val attributeStore = AccumuloAttributeStore(instance.connector)
    val collectionLayerReader = AccumuloCollectionLayerReader(instance)
//    val reader = AccumuloLayerReader(instance)
    attributeStore.layerIds
    //渲染色带
    val etColormap = "0:ffffe5ff;1:f7fcb9ff;2:d9f0a3ff;3:addd8eff;4:78c679ff;5:41ab5dff;6:238443ff"
    val colorMapForRender = ColorMap.fromStringDouble(etColormap).get

    def rasterFunction(): Tile => Tile = {
      tile: Tile => tile.convert(DoubleConstantNoDataCellType)
    }

    def pngAsHttpResponse(png: Png): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))

    implicit val system: ActorSystem = ActorSystem("my-system")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    // 定义路由规则，响应客户端请求
    val route =
      pathPrefix("map" / IntNumber) { zoom =>
        val fn: Tile => Tile = rasterFunction()
        val layerId = LayerId("layer_"+args(0), zoom)
        path(IntNumber / IntNumber) { (x, y) =>
          get {
            val valueReader: Reader[SpatialKey, Tile] = AccumuloValueReader(
              instance, attributeStore, layerId)
            val metadata = attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
//            val bounds = metadata.bounds.get
//            val key1 = bounds.minKey
//            val key2 = bounds.maxKey
            val tile: Tile = valueReader.read(SpatialKey(x,y))
            val product: Tile = fn(tile)
            val cm: ColorMap = colorMapForRender
            val png: Png = product.renderPng(cm)

            //            val pngBytes: Array[Byte] = tile.renderPng().bytes // 如果为空，返回空数组
            // 返回 HTTP 响应，内容类型为 image/png，内容为字节数组
            complete(HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))
          }
        } ~
          // Polygonal summary route:
          pathPrefix("summary") {
            pathEndOrSingleSlash {
              post {
                entity(as[String]) { geoJson =>
                  val poly = geoJson.parseGeoJson[Polygon]

                  // Leaflet produces polygon in LatLng, we need to reproject it to layer CRS
                  val layerMetadata = attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
                  val queryPoly = poly.reproject(LatLng, layerMetadata.crs)

                  // Query all tiles that intersect the polygon and build histogram
                  val queryHist = collectionLayerReader
                    .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
                    .where(Intersects(queryPoly))
                    .result // all intersecting tiles have been fetched at this point
                    .withContext(_.mapValues(fn))
                    .polygonalHistogramDouble(queryPoly)

                  val result: (Double, Double) =
                    queryHist.minMaxValues().getOrElse((Double.NaN, Double.NaN))

                  complete(result)
                }
              }
            }
          }
        }


    // 启动 HTTP 服务器，监听端口9090
    val bindingFuture = Http().bindAndHandle(route, "192.168.163.131", 9090)

    println(s"Server online at http://192.168.163.131:9090/\nPress RETURN to stop...")
    scala.io.StdIn.readLine() // 等待用户输入回车键停止服务器

    bindingFuture
      .flatMap(_.unbind()) // 触发端口解绑操作
      .onComplete(_ => system.terminate()) // 关闭 Actor 系统
  }
}