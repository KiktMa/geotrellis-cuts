# GeoTrellis-Cuts: 分布式栅格数据和点云存储方案

![Project Banner](project_banner.png) <!-- 替换为您的项目横幅图片 -->

欢迎来到 GeoTrellis-Cuts 项目！这是一个致力于优化栅格数据和点云数据存储的分布式方案，利用 GeoSOT 模型构建金字塔和多级存储，将数据高效地存储于分布式 Accumulo 数据库中。

## 项目简介

GeoTrellis-Cuts 项目旨在解决遥感影像和点云数据存储的性能瓶颈，通过构建基于 Hadoop 集群的存储架构以及利用 GeoSOT 和 GeoSOT-3D 编码进行索引，提高数据的存储和读取效率。

项目包含两个主要部分：
- **栅格数据存储**：利用 Geotrellis 分布式计算引擎建立轻量级金字塔模型，将经过预处理的遥感影像存储于 HDFS 中，并通过 GeoSOT 编码添加空间索引，最终存储于分布式数据库 Accumulo。
- **点云数据存储**：采用基于 GeoSOT-3D 索引的方法，对点云数据进行滤波处理，添加 GeoSOT-3D 编码作为索引，并利用 Spark 将数据存储于分布式 Accumulo 数据库。

## 技术亮点

- 构建基于 Hadoop 集群的分布式遥感影像存储架构
- 利用 Geotrellis 分布式计算引擎建立轻量级金字塔模型
- 使用 GeoSOT 和 GeoSOT-3D 编码进行空间索引
- 高效存储和读取数据于分布式 Accumulo 数据库
- 借助 Akka 网络通讯框架发布遥感影像数据为网络服务
- 在 Cesium 地球中展示遥感影像数据和点云数据

## 项目结果

通过实验验证，GeoTrellis-Cuts 在栅格数据和点云数据存储方面均取得了显著的性能提升。相较于传统方法，我们的方案能够更高效地处理大规模遥感影像和点云数据，节省时间和资源。

## 使用示例

```bash
# 克隆项目仓库
git clone https://github.com/yourusername/geotrellis-cuts.git
cd geotrellis-cuts

# 将项目打包上传Linux中
./bin/spark-submit --class com.spark.demo.Cuts --master yarn /jar包目录/spark_demo-0.1.0-SNAPSHOT.jar "hdfs://Hadoop集群master节点ip:port/栅格路径" "testcode" "17" "14"

# 将金字塔模型发布为TMS服务
./bin/spark-submit --class com.spark.demo.read.WebServer --master yarn /jar包目录/spark_demo-0.1.0-SNAPSHOT.jar "金字塔模型在accumulo中的存储表名"
```

## 前端展示demo
[CesiumDemo](https://github.com/KiktMa/CesiumDemo)
