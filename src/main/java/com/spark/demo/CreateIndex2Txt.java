package com.spark.demo;

import com.spark.demo.geosot.GeoSot;
import java.io.*;
public class CreateIndex2Txt {
    public static void main(String[] args) {

        double height = 0;
        int level = 18;
        try {
            // 读取本地txt文件
            File inputFile = new File("D:\\test_tif\\lonlat\\input.txt");
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));

            // 创建新的txt文件
            File outputFile = new File("D:\\test_tif\\lonlat\\lonlat.txt");
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

            // 读取每一行并计算相加结果
            String line;
            while ((line = reader.readLine()) != null) {
                String[] elements = line.split(" ");
                double lat = Double.parseDouble(elements[1]);
                double lon = Double.parseDouble(elements[2]);
                String hexCode = GeoSot.INSTANCE.getHexCode(lat, lon, height, level);

                // 将结果写入新的txt文件
                writer.write(hexCode + "\n");
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
