# geotrellis-cuts
**关于使用geotrellis API实现对hdfs中的栅格数据进行切片，并存储到accumulo分布式数据库中**

由于这里使用的是yarn模式的spark集群来运行，所以需要将程序打包放到集群中去以spark-submit的形式进行提交

**注意：**
在使用spark-submit提交spark任务时需要将程序相关的所有jar包都放到集群spark的lib目录下，否则会报错