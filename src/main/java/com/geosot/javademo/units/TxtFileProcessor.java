package com.geosot.javademo.units;

import java.io.*;
import java.util.*;

/**
 * 将txt文件中具有相同编码的点移除，保留一个
 */
public class TxtFileProcessor {
    public static void main(String[] args) {
        String inputFilePath = "D:\\JavaConsist\\MapData\\180m_pointcloud\\0\\new\\corrected-LJYY-Cloud-1-0-0 - Clean.txt\\part-00000-c06037ec-654e-4c01-a5f2-0bbd70bad369-c000.txt";
        String outputFilePath = "D:\\JavaConsist\\MapData\\180m_pointcloud\\0\\new\\part-all-clean.txt";

        Map<String, Integer> lastStringsCount = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath));
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath))) {

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" ");
                String lastString = parts[parts.length - 1];
                int count = lastStringsCount.getOrDefault(lastString, 0);
                if (count == 0) {
                    lastStringsCount.put(lastString, 1);
                    bw.write(line);
                    bw.newLine();
                } else {
                    lastStringsCount.put(lastString, count + 1);
                }
            }

            bw.flush(); // 确保所有数据都被写入文件
            System.out.println("处理完成");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 将十六进制字符串转换为字节数组
    public static byte[] hexStringToByteArray(String hexString) {
        int len = hexString.length();
        byte[] byteArray = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            byteArray[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }
        return byteArray;
    }

    // 将字节数组转换为二进制数组
    public static boolean[] byteArrayToBinaryArray(byte[] byteArray) {
        boolean[] binaryArray = new boolean[byteArray.length * 8];
        int index = 0;
        for (byte b : byteArray) {
            for (int bitIndex = 7; bitIndex >= 0; bitIndex--) {
                binaryArray[index++] = (b & (1 << bitIndex)) != 0;
            }
        }
        return binaryArray;
    }

}
