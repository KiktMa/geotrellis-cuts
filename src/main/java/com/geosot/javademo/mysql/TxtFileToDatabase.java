package com.geosot.javademo.mysql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TxtFileToDatabase {

    public static void main(String[] args) {
        String filePath = "path/to/your_file.txt"; // 替换为你的txt文件路径
        String dbUrl = "jdbc:mysql://localhost:3306/your_database_name"; // 替换为你的数据库连接URL
        String username = "your_username"; // 替换为你的数据库用户名
        String password = "your_password"; // 替换为你的数据库密码

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath));
             Connection connection = DriverManager.getConnection(dbUrl, username, password)) {

            String insertQuery = "INSERT INTO your_table_name (code, x, y, z, lat, lon) VALUES (?, ?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

            String line;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split("\\s+"); // 假设数据以空白字符分隔
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
                    System.out.println("Invalid data format: " + line);
                }
            }

            System.out.println("Data insertion complete!");

        } catch (IOException e) {
            System.out.println("Error reading the file: " + e.getMessage());
        } catch (SQLException e) {
            System.out.println("Error connecting to the database or executing query: " + e.getMessage());
        }
    }
}