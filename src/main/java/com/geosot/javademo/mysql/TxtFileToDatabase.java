package com.geosot.javademo.mysql;

import com.geosot.javademo.units.MySQLConnect;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 将点云数据存储到mysql数据库中
 * 一些存储实验
 */
public class TxtFileToDatabase {

    public static void main(String[] args) {
        String filePath = "D:\\JavaConsist\\MapData\\180m_pointcloud\\0\\new\\part-all-clean.txt";
        long start = System.currentTimeMillis();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath));
             Connection connection = MySQLConnect.getConnection()) {

            String insertQuery = "INSERT INTO las_0 (code, x, y, z, lat, lon) VALUES (?, ?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

            String line;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(" "); // 假设数据以空白字符分隔
                if (data.length == 6) {
                    String code = data[5];
                    double x = Double.parseDouble(data[0]);
                    double y = Double.parseDouble(data[1]);
                    double z = Double.parseDouble(data[2]);
                    double lat = Double.parseDouble(data[3]);
                    double lon = Double.parseDouble(data[4]);

                    preparedStatement.setString(1, code);
                    preparedStatement.setDouble(2, x);
                    preparedStatement.setDouble(3, y);
                    preparedStatement.setDouble(4, z);
                    preparedStatement.setDouble(5, lat);
                    preparedStatement.setDouble(6, lon);

                    preparedStatement.executeUpdate();
                } else {
                    System.out.println("数据格式无效: " + line);
                }
            }
            connection.commit();
            System.out.println((System.currentTimeMillis() - start)+"ms");
            System.out.println("插入数据完成");

        } catch (IOException e) {
            System.out.println("文件读取错误: " + e.getMessage());
        } catch (SQLException e) {
            System.out.println("连接数据库失败: " + e.getMessage());
        }
    }
}