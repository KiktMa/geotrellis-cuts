# geotrellis-cuts
**关于使用geotrellis API实现对hdfs中的栅格数据进行切片，并存储到accumulo分布式数据库中**

由于这里使用的是yarn模式的spark集群来运行，所以需要将程序打包放到集群中去以spark-submit的形式进行提交

**注意：**
在使用spark-submit提交spark任务时需要将程序相关的所有jar包都放到集群spark的lib目录下，否则会报错。
我试着使用maven-assembly-plugin插件将所有依赖的包一起打成一个jar包，上传到spark集群后还是会出错，还是通过自己手动将所有依赖的jar包上传到spark集群才不报错
<!-- 这里我弄了好久才将所有依赖的包上传 -->

---

### 最近在做将空间填充曲线的索引值改成自己定义的网格码

- 尝试了许多方法，空间索引的SpatialKey在源码中必须是两个值来代表（比如Row，Col）
- 