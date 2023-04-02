package com.spark.demo.read

import geotrellis.raster.Tile
import geotrellis.spark.io.accumulo.{AccumuloAttributeStore, AccumuloInstance, AccumuloKeyEncoder, AccumuloLayerReader}
import geotrellis.spark.io.{SpatialKeyFormat, tileLayerMetadataFormat}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.vector.Extent
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{BatchWriter, BatchWriterConfig, ZooKeeperInstance}
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.accumulo.core.data.Range

object AccumuloReader {
  val token = new PasswordToken("root")
  val user = "root"
  val instanceName = "accumulo"
  val zooServers = "node1:2181,node2:2181,node3:2181"
  val table = "catalog"
  def main(args: Array[String]) {
    //    write
    val startTime = System.currentTimeMillis()
    read
    val endTime = System.currentTimeMillis()
    val durationMillis = endTime - startTime
    val durationSeconds = durationMillis / 1000.0
    println(s"Duration: $durationSeconds seconds")
  }

  def read = {
    val conn = getConn
    val auths = Authorizations.EMPTY// new Authorizations("Valid")
    val scanner = conn.createScanner(table, auths)

    // 创建 Accumulo 数据库连接
//    implicit val instance: AccumuloInstance = AccumuloInstance("accumulo", "node1:2181,node2:2181,node3:2181",
//      "root", new PasswordToken("root"))
//    val attributeStore = AccumuloAttributeStore(instance.connector)
//    val reader = AccumuloLayerReader(attributeStore)(SparkContext.getOrCreate(),instance)
//    val layerId: LayerId = LayerId("testid", 10) // 图层名称和缩放级别

    // 读取指定图层和缩放级别的数据
//    val rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
//    val masked = rdd.mask(Extent(0, 0, 0, 0))
//    val stitch = masked.stitch()
//    val tile = stitch.tile
//    println(rdd)
//    AccumuloLayerCa
    val startKey = new Text("testid:18:10224133.430:3198750.363")
    val endKey = new Text("testid:18:10226885.809:3199985.617")
    val range = new Range(startKey, true, endKey, false)
    scanner.setRange(range)
//        scanner.fetchColumnFamily()
//        println(scanner.iterator().next().getKey)
    val iter = scanner.iterator()
    while (iter.hasNext){
      var item = iter.next()
      val bytes = item.getValue.get()
      println(bytes)
//      //Accumulo中数据存放在table中，分为Key Value，其中Key又包含RowID和Column，Column包含Family Qualifier Visibility
      println(s"key  row:${item.getKey.getRow} fam:${item.getKey.getColumnFamily} qua:${item.getKey.getColumnQualifier} value:${item.getValue}")
    }
//        for(entry <- scanner) {
//          println(entry.getKey + " is " + entry.getValue)
//        }
  }

  def write {
    val mutation = createMutation
    val writer = getWriter
    writer.addMutation(mutation)
    //    writer.flush()
    writer.close
  }

  def createMutation = {
    val rowID = new Text("row2")
    val colFam = new Text("myColFam")
    val colQual = new Text("myColQual")
    //  val colVis = new ColumnVisibility("public")  //不需要加入可见性
    var timstamp = System.currentTimeMillis
    val value = new Value("myValue".getBytes)
    val mutation = new Mutation(rowID)
    mutation.put(colFam, colQual, timstamp, value)
    mutation
  }

  def getConn = {
    val inst = new ZooKeeperInstance(instanceName, zooServers)
    val conn = inst.getConnector("root", token)
    conn
  }

  def getWriter() = {
    val conn = getConn
    val config = new BatchWriterConfig
    config.setMaxMemory(10000000L)
    val writer: BatchWriter = conn.createBatchWriter(table, config)
    writer
  }
}