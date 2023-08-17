package com.geosot.javademo.units;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class LasMetadataReader {
    public static void main(String[] args) {
        String lasFilePath = "D:\\JavaConsist\\MapData\\180m_pointcloud\\9\\corrected-LJYY-Cloud-1-0-9.las";  // 替换为实际的LAS文件路径
        try {
            FileInputStream fileInputStream = new FileInputStream(new File(lasFilePath));
            byte[] headerData = new byte[227];  // LAS文件头部长度通常为227个字节
            fileInputStream.read(headerData);

            // 输出LAS文件的版本号
            short versionMajor = byteArrayToShort(headerData, 24);
            short versionMinor = byteArrayToShort(headerData, 26);
            System.out.println("LAS Version: " + versionMajor + "." + versionMinor);

            // 输出坐标偏移量
            double offsetX = byteArrayToDouble(headerData, 131);
            double offsetY = byteArrayToDouble(headerData, 139);
            double offsetZ = byteArrayToDouble(headerData, 147);
            System.out.println("X Offset: " + offsetX);
            System.out.println("Y Offset: " + offsetY);
            System.out.println("Z Offset: " + offsetZ);

            // 输出部分元数据信息（仅示例）
            String fileSignature = new String(headerData, 0, 4);
            int pointDataOffset = byteArrayToInt(headerData, 96);
            int pointCount = byteArrayToInt(headerData, 107);
            System.out.println("File Signature: " + fileSignature);
            System.out.println("Point Data Offset: " + pointDataOffset);
            System.out.println("Point Count: " + pointCount);

            // 在这里可以继续解析更多的元数据信息

            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static short byteArrayToShort(byte[] bytes, int offset) {
        return (short) ((bytes[offset] & 0xFF) | ((bytes[offset + 1] & 0xFF) << 8));
    }

    private static int byteArrayToInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) |
                ((bytes[offset + 1] & 0xFF) << 8) |
                ((bytes[offset + 2] & 0xFF) << 16) |
                ((bytes[offset + 3] & 0xFF) << 24));
    }

    private static double byteArrayToDouble(byte[] bytes, int offset) {
        long longBits = ((long) bytes[offset] & 0xFF) |
                (((long) bytes[offset + 1] & 0xFF) << 8) |
                (((long) bytes[offset + 2] & 0xFF) << 16) |
                (((long) bytes[offset + 3] & 0xFF) << 24) |
                (((long) bytes[offset + 4] & 0xFF) << 32) |
                (((long) bytes[offset + 5] & 0xFF) << 40) |
                (((long) bytes[offset + 6] & 0xFF) << 48) |
                (((long) bytes[offset + 7] & 0xFF) << 56);
        return Double.longBitsToDouble(longBits);
    }
}