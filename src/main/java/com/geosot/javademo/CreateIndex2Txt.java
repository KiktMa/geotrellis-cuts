package com.geosot.javademo;

import com.spark.demo.index.GeoSot;
import com.sun.jna.Pointer;

import java.io.*;
public class CreateIndex2Txt {
    public static void main(String[] args) {

        double height = 0;
        byte level = 18;
        byte[] geoID = new byte[4];
        try {
            // 读取本地txt文件
            File inputFile = new File("D:\\test_tif\\lonlat\\input.txt");
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));

            // 创建新的txt文件
            File outputFile = new File("D:\\test_tif\\lonlat\\lonlat222.txt");
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

            // 读取每一行并计算相加结果
            String line;
            while ((line = reader.readLine()) != null) {
                String[] elements = line.split(" ");
                double lat = Double.parseDouble(elements[1]);
                double lon = Double.parseDouble(elements[2]);
                GeoSot.INSTANCE.PointGridIdentify3D(lat, lon, height, level, geoID);
                Pointer changeCode = GeoSot.INSTANCE.createChangeCode(geoID, 18);
                String code = GeoSot.INSTANCE.getHexOneDimensionalCode(changeCode);

//                for (byte b : geoID) {
                writer.write(code + "\n");
//                }
                GeoSot.INSTANCE.destroyChangeCode(changeCode);
//                writer.write("\n ");
//                GeoSot.INSTANCE.freeMemory(hexCode);
            }
            // 关闭输入输出流
            reader.close();
            writer.close();
            System.out.println("操作完成！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}