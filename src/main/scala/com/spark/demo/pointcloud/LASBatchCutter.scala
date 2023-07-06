package com.spark.demo.pointcloud

import io.pdal._
import org.apache.spark.sql.SparkSession

object LASBatchCutter {
  def main(args: Array[String]): Unit = {

    val json =
      """
        |{
        |  "pipeline":[
        |    {
        |      "filename":"D:\\JavaConsist\\MapData\\180m_pointcloud\\corrected-LJYY-Cloud-1-0-9.las",
        |      "spatialreference":"EPSG:4326"
        |    },
        |    {
        |      "type": "filters.reprojection",
        |      "out_srs": "EPSG:3857"
        |    },
        |    {
        |       "type": "filters.delaunay"
        |    }
        |  ]
        |}
    """.stripMargin

//    val spark = SparkSession.builder().appName("LAS Reader").getOrCreate()

//    val dataFrame = spark.read.format("parquet").load("D:\\JavaConsist\\MapData\\180m_pointcloud\\corrected-LJYY-Cloud-1-0-0.las")

    val pipeline = new Pipeline(json)
    pipeline.validate() // check if our JSON and options were good
    pipeline.setLogLevel(8) // make it really noisy
    pipeline.execute() // execute the pipeline
    val metadata: String = pipeline.getMetadata() // retrieve metadata
    println(metadata)
    val pvs: PointViewIterator = pipeline.getPointViews() // iterator over PointViews
    val pv: PointView = pvs.next() // let's take the first PointView

    // load all points into JVM memory
    // PointCloud provides operations on PDAL points that
    // are loaded in this case into JVM memory as a single Array[Byte]
    val pointCloud: PointCloud = pv.getPointCloud()
    val x: Double = pointCloud.getDouble(0, DimType.X) // get a point with PointId = 0 and only a single dimensions

    // in some cases it is not neccesary to load everything into JVM memory
    // so it is possible to get only required points directly from the PointView
    val y: Double = pv.getDouble(0, DimType.Y)

    // it is also possible to get access to the triangular mesh generated via PDAL
    val mesh: TriangularMesh = pv.getTriangularMesh()
    // the output is an Array of Triangles
    // Each Triangle contains PointIds from the PDAL point table
    val triangles: Array[Triangle] = mesh.asArray

    pv.close()
    pipeline.close()
  }
}
