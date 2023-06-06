package com.spark.demo.index

import geotrellis.spark.io.index.zcurve.ZSpatialKeyIndex
import geotrellis.spark.{KeyBounds, SpatialKey}
import geotrellis.spark.io.index.{KeyIndex, KeyIndexMethod}
import geotrellis.vector.Extent

class GeosotIndex extends KeyIndexMethod[SpatialKey]{
  override def createIndex(keyBounds: KeyBounds[SpatialKey]): KeyIndex[SpatialKey] = ???

  // 将spatialkey转换为瓦片左上角经纬度信息
//  def zhuanHuan(keyBounds: KeyBounds[SpatialKey]): Seq[Double] = {
//    val zSpatialKeyIndex = new ZSpatialKeyIndex(keyBounds)
//    val tileExtent: Extent = zSpatialKeyIndex.toIndex(SpatialKey(1, 5))
//    val polygon = tileExtent.toPolygon()
//    val centroid = polygon.jtsGeom.getCentroid
//    val (lon, lat): (Double, Double) = centroid.getX -> centroid.getY
//    (lon,lat)
//  }
}
