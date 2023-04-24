package com.spark.demo.read

import geotrellis.spark.LayerId
import geotrellis.spark.SpaceTimeKey.Boundable
import geotrellis.spark.io.SpaceTimeKeyFormat
import geotrellis.spark.io.accumulo.{AccumuloInstance, AccumuloLayerReader}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.SparkContext

object ReadTiffFromAccumulo {
  def main(args: Array[String]): Unit = {

  }

  def readTiffFromAccumulo(implicit sparkContext: SparkContext, tableName: String): Unit = {
    val strId = LayerId("layer_" + tableName, 18)
    val instance = AccumuloInstance("accumulo", "", "root", new PasswordToken("root"))
    implicit val layerReader: AccumuloLayerReader = AccumuloLayerReader(instance)
  }
}
