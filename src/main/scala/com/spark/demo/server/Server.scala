package com.spark.demo.server

import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.io.{FilteringLayerReader, ValueReader, _}
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

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import geotrellis.proj4.{CRS, LatLng}

object Server {
  /*
  这里是将金字塔模型的瓦片存储在文件系统中时利用akka发布图层的服务
   */
  implicit val system = ActorSystem("tutorial-system")
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()
  val logger = Logging(system, getClass)

  val catalogPath = new java.io.File("data/catalog1").toURI
  //创建存储区
  val attributeStore: AttributeStore =
    AttributeStore(catalogPath)
  //创建valuereader，用来读取每个tile的value
  val valueReader: ValueReader[LayerId] =
    ValueReader(attributeStore, catalogPath)
  //渲染色带
  val etColormap = "0:ffffe5ff;1:f7fcb9ff;2:d9f0a3ff;3:addd8eff;4:78c679ff;5:41ab5dff;6:238443ff"
  val colorMapForRender: ColorMap = ColorMap.fromStringDouble(etColormap).get

  def pngAsHttpResponse(png: Png): HttpResponse =
    HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))

  def root =
    pathPrefix(Segment / IntNumber ) {
      (_, zoom) =>
        val fn: Tile => Tile = rasterFunction()
        // ZXY route:
        pathPrefix(IntNumber / IntNumber) { (x, y) =>
          complete {
            Future {
              // Read in the tile at the given z/x/y coordinates.
              val tileOpt: Option[Tile] =
                try {
                  val reader = valueReader.reader[SpatialKey, Tile](LayerId("landsat", zoom))
                  Some(reader.read(x, y))
                } catch {
                  case _: ValueNotFoundError =>
                    None
                }

              for (tile <- tileOpt) yield {
                val product: Tile = fn(tile)
                val cm: ColorMap = colorMapForRender
                val png: Png = product.renderPng(cm)
                pngAsHttpResponse(png)
              }
            }
          }
        }
    } ~
      // Static content routes:
      pathEndOrSingleSlash {
        getFromFile("static/index.html")
      } ~
      pathPrefix("") {
        getFromDirectory("static")
      }

  Http().bindAndHandle(root, "192.168.163.131", 8880)


  /** raster transformation to perform at request time */
  def rasterFunction(): Tile => Tile = {
    tile: Tile => tile.convert(DoubleConstantNoDataCellType)
  }
}