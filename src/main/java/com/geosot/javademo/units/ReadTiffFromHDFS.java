package com.geosot.javademo.units;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 从HDFS中读取下载栅格数据到本地环境中
 */
public class ReadTiffFromHDFS {
    public static void main(String[] args) throws IOException {
        // 创建spark连接
        SparkConf conf = new SparkConf().setAppName("caijian_zhuanhuan");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 设置hadoop配置文件
        Configuration hadoopConf = sc.hadoopConfiguration();
        hadoopConf.set("fs.defaultFS", "hdfs://node1:9000");

        FileSystem fs = FileSystem.get(hadoopConf);

        // 读取hdfs中的栅格数据
        Path inputPath = new Path("hdfs://node1:9000/tiff/caijian_zhuanhuan.tif");
        InputStream in = fs.open(inputPath);
        BufferedImage image = ImageIO.read(in);
        in.close();

        // 保存tiff文件到本地文件系统中
        Path outputPath = new Path("/app/tif/test.tif");
        OutputStream out = fs.create(outputPath);
        ImageIO.write(image, "tiff", out);
        out.close();

        sc.stop();
    }
}
