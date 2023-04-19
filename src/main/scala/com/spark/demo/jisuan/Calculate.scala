//package com.spark.demo.jisuan
//
//import geotrellis.proj4.LatLng
//import geotrellis.raster.density.KernelStamper
//import geotrellis.raster.io.geotiff.GeoTiff
//import geotrellis.raster.mapalgebra.focal.Kernel
//import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
//import geotrellis.raster.{ArrayTile, DoubleCellType, MutableArrayTile, RasterExtent, Tile, TileLayout, isNoData}
//import geotrellis.spark.{ContextRDD, KeyBounds, SpatialKey, TileLayerMetadata}
//import geotrellis.spark.tiling.LayoutDefinition
//import geotrellis.vector.{Extent, PointFeature}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//object Calculate {
//
//  def ncdensityrdd(): Unit = {
//    implicit var extent: Extent = Extent(10237450.287, 3196984.713, 10239779.287, 3199173.213)
//    val conf = new SparkConf().setMaster("local").setAppName("Kernel Density")
//    val sc = new SparkContext(conf)
//    val tl = TileLayout(7, 4, 100, 100)
//    val ld = LayoutDefinition(extent, tl)
//
//    val kernelWidth = 9
//
//    def ptfToExtent[D](p: PointFeature[D]) = pointFeatureToExtent(kernelWidth, ld, p)
//
//    def pointFeatureToExtent[D](kwidth: Double, ld: LayoutDefinition, ptf: PointFeature[D]): Extent = {
//      val p = ptf.geom
//
//      Extent(p.x - kwidth * ld.cellwidth / 2,
//        p.y - kwidth * ld.cellheight / 2,
//        p.x + kwidth * ld.cellwidth / 2,
//        p.y + kwidth * ld.cellheight / 2)
//    }
//
//    def ptfToSpatialKey[D](ptf: PointFeature[D]): Seq[(SpatialKey, PointFeature[D])] = {
//      val ptextent = ptfToExtent(ptf)
//      val gridBounds = ld.mapTransform(ptextent)
//
//      for {
//        tup <- gridBounds.coordsIter.toSeq
//        if tup._2 < tl.totalRows
//        if tup._1 < tl.totalCols
//      } yield (SpatialKey(tup._1, tup._2), ptf)
//    }
//
//    val pointRdd = sc.parallelize(Seq[pts], 10)
//
//    def stampPointFeature(
//                           tile: MutableArrayTile,
//                           tup: (SpatialKey, PointFeature[Double])
//                         ): MutableArrayTile = {
//      val (spatialKey, pointFeature) = tup
//      val tileExtent = ld.mapTransform(spatialKey)
//      val re = RasterExtent(tileExtent, tile)
//      val result = tile.copy.asInstanceOf[MutableArrayTile]
//      val kernelWidth: Int = 9
//
//      /* Gaussian kernel with std. deviation 1.5, amplitude 25 */
//      val kern: Kernel = Kernel.gaussian(kernelWidth, 1.5, 25)
//      KernelStamper(result, kern)
//        .stampKernelDouble(re.mapToGrid(pointFeature.geom), pointFeature.data)
//
//      result
//    }
//
//    def sumTiles(t1: MutableArrayTile, t2: MutableArrayTile): MutableArrayTile = {
//      Adder(t1, t2).asInstanceOf[MutableArrayTile]
//    }
//
//    val tileRdd: RDD[(SpatialKey, Tile)] =
//      pointRdd
//        .flatMap(ptfToSpatialKey)
//        .mapPartitions({ partition =>
//          partition.map { case (spatialKey, pointFeature) =>
//            (spatialKey, (spatialKey, pointFeature))
//          }
//        }, preservesPartitioning = true)
//        .aggregateByKey(ArrayTile.empty(DoubleCellType, ld.tileCols, ld.tileRows))(stampPointFeature, sumTiles)
//        .mapValues { tile: MutableArrayTile => tile.asInstanceOf[Tile] }
//
//    //    tileRdd.foreach(t=>{
//    //      GeoTiff(t._2, extent, LatLng).write("/cephfs/test/density/%s%s.tif".format(t._1.row,t._1.col))
//    //    })
//
//    val metadata = TileLayerMetadata(DoubleCellType,
//      ld,
//      ld.extent,
//      LatLng,
//      KeyBounds(SpatialKey(0, 0),
//        SpatialKey(ld.layoutCols - 1,
//          ld.layoutRows - 1)))
//
//    val resultRdd = ContextRDD(tileRdd, metadata)
//
//    resultRdd.foreach(t => {
//      GeoTiff(t._2, extent, LatLng).write("/cephfs/test/density/%s%s.tif".format(t._1.row, t._1.col))
//    })
//
//    val stitchtile = resultRdd.stitch.tile
//    GeoTiff(stitchtile, extent, LatLng).write("/cephfs/test/density/all.tif")
//    sc.stop()
//  }
//
//  object Adder extends LocalTileBinaryOp {
//    def combine(z1: Int, z2: Int) = {
//      if (isNoData(z1)) {
//        z2
//      } else if (isNoData(z2)) {
//        z1
//      } else {
//        z1 + z2
//      }
//    }
//
//    def combine(r1: Double, r2: Double) = {
//      if (isNoData(r1)) {
//        r2
//      } else if (isNoData(r2)) {
//        r1
//      } else {
//        r1 + r2
//      }
//    }
//  }
//}
