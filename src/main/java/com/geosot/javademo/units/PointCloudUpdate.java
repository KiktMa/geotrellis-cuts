package com.geosot.javademo.units;

import com.geosot.javademo.geosot.ChangeCode;
import com.geosot.javademo.geosot.CoordinateChange;
import com.spark.demo.index.GeoSot;

import java.io.*;

/**
 * @Author: june
 * @Date: 2023/9/16 20:14
 */
public class PointCloudUpdate {
    public static void main(String[] args) {
        // 输入文件路径和输出文件路径
        String inputPath = "C:\\Users\\mj\\Desktop\\paper\\pointcloud\\点云数据\\birmingham_block_1 - Cloud.txt";
        String outputPath = "C:\\Users\\mj\\Desktop\\paper\\pointcloud\\点云数据\\birmingham_block_new - Cloud.txt";

        try {
            long start = System.currentTimeMillis();
            // 创建文件输入流
            BufferedReader reader = new BufferedReader(new FileReader(inputPath));

            // 创建文件输出流
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath));

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(" ");
                double x = Double.parseDouble(values[0]);
                double y = Double.parseDouble(values[1]);
                double z = Double.parseDouble(values[2]);
                Double[] lonlat = CoordinateChange.getCoordinate(x, y);
                byte[] geoarr = new byte[32];
                GeoSot.INSTANCE.PointGridIdentify3D(lonlat[0], lonlat[1], z, (byte) 32, geoarr);
                ChangeCode code = new ChangeCode(geoarr, 32);
                String geocode = code.getHexOneDimensionalCode();
                String resultLine = String.format("%f %f %f %f %f %s", x, y, z, lonlat[0], lonlat[1], geocode);
                writer.write(resultLine);
                writer.newLine();
            }
            System.out.println(System.currentTimeMillis()-start+"ms");
            // 关闭文件流
            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
