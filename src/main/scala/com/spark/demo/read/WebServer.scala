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
import akka.http.scaladsl.model.headers.{`Access-Control-Allow-Credentials`, `Access-Control-Allow-Headers`, `Access-Control-Max-Age`}
import spray.json._
import akka.stream.{ActorMaterializer, Materializer}
import spray.json.DefaultJsonProtocol._

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloCollectionLayerReader, AccumuloInstance, AccumuloValueReader}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.{SparkConf, SparkContext}

object WebServer extends CorsSupport {
  /**
   * 这里需在提交spark任务时加入一个参数
   * @param args args(0)表示瓦片存储的表的表名
   */
  val instanceName = "accumulo"
  val zookeepers = "node1:2181,node2:2181,node3:2181"
  val user = "root"
  val password = "root"

  val sparkConf = new SparkConf().setAppName("WebServer").setMaster("yarn")
  implicit val sparkContext: SparkContext = new SparkContext(sparkConf)
  // 创建 Accumulo 数据库连接
  implicit val instance: AccumuloInstance = AccumuloInstance("accumulo", "node1:2181,node2:2181,node3:2181",
    "root", new PasswordToken("root"))
  implicit val attributeStore = AccumuloAttributeStore(instance.connector)
  val collectionLayerReader = AccumuloCollectionLayerReader(instance)
  //    val reader = AccumuloLayerReader(instance)
  attributeStore.layerIds
  //渲染色带
//  val etColormap = "0:fefefeff; 1:e0e0e0ff;2:fea67eff; 3:37a700ff; 4:f9e500ff; 5:97e500ff; 6:79b5f4ff;" +
//    "7:818181ff; 8:999dd2ff; 9:955895ff; 10:fefe00ff; 11:d89447ff; 12:d34a1aff; 13:fefefeff; 14:000000ff"
//  val colorMapForRender = ColorMap.fromStringDouble(etColormap).get
  val colorMapForRender = ColorMap(Map(0 -> RGB(254,254,254), 1 -> RGB(224,224,224),
  2 -> RGB(254,166,126),3 -> RGB(55,167,0),4 -> RGB(249,229,0),5 -> RGB(151,229,0),6 -> RGB(121,181,244),
  7 -> RGB(129,129,129),8 -> RGB(153,157,210),9 -> RGB(149,88,149),10 -> RGB(254,254,0),11 -> RGB(216,149,71),
  12 -> RGB(211,74,26),13 -> RGB(254,254,254),14 -> RGB(0,0,0)))

  def rasterFunction(): Tile => Tile = {
    tile: Tile => tile.convert(DoubleConstantNoDataCellType)
  }

  def pngAsHttpResponse(png: Png): HttpResponse =
    HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))

  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override val corsAllowOrigins: List[String] = List("*")

  override val corsAllowedHeaders: List[String] = List("Origin", "X-Requested-With", "Content-Type", "Accept", "Accept-Encoding", "Accept-Language", "Host", "Referer", "User-Agent")

  override val corsAllowCredentials: Boolean = true

  override val optionsCorsHeaders: List[HttpHeader] = List[HttpHeader](
    `Access-Control-Allow-Headers`(corsAllowedHeaders.mkString(", ")),
    `Access-Control-Max-Age`(60 * 60 * 24 * 20), // cache pre-flight response for 20 days
    `Access-Control-Allow-Credentials`(corsAllowCredentials)
  )
  def main(args: Array[String]): Unit = {

    val route = cors {
      pathPrefix("map" / IntNumber) { zoom =>
        val fn: Tile => Tile = rasterFunction()
        val layerId = LayerId("layer_" + args(0), zoom)
        path(IntNumber / IntNumber) { (x, y) =>
          get {
            val valueReader: Reader[SpatialKey, Tile] = AccumuloValueReader(
              instance, attributeStore, layerId)
            val metadata = attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
            //            val bounds = metadata.bounds.get
            //            val key1 = bounds.minKey
            //            val key2 = bounds.maxKey
            val tile: Tile = valueReader.read(SpatialKey(x, y))

            //            val product: Tile = fn(tile)
            //            val cm: ColorMap = colorMapForRender
            //            val png: Png = product.renderPng(cm)
            //            val bytes = tile.toBytes()

            val pngBytes: Array[Byte] = tile.renderPng().bytes // 如果为空，返回空数组
            // 返回 HTTP 响应，内容类型为 image/png，内容为字节数组
            complete(HttpEntity(ContentType(MediaTypes.`image/png`), pngBytes))
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