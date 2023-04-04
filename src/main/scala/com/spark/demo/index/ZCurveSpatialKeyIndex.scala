package com.spark.demo.index

import geotrellis.spark._
import geotrellis.spark.io.index._
import geotrellis.spark.io.index.zcurve._

object ZCurveSpatialKeyIndex extends KeyIndexMethod[SpatialKey] {
  override def createIndex(keyBounds: KeyBounds[SpatialKey]): KeyIndex[SpatialKey] = new ZCurveKeyIndex(keyBounds)

  class ZCurveKeyIndex(keyBounds: KeyBounds[SpatialKey]) extends KeyIndex[SpatialKey] {
    def toIndex(key: SpatialKey): BigInt =
      Z2(key.col, key.row)

    def indexRanges(keyRange: (SpatialKey, SpatialKey)): Seq[(BigInt, BigInt)] =
      Z2.zranges(keyRange._1.col, keyRange._1.row, keyRange._2.col, keyRange._2.row)

    def keyBounds: KeyBounds[SpatialKey] = keyBounds

    def toKey(index: BigInt): SpatialKey =
      Z2.z2ToColRow(index.bigInteger.longValue()) match {
        case (col, row) => SpatialKey(col.toInt, row.toInt)
      }
  }
}
