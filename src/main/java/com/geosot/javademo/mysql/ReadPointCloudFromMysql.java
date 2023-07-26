package com.geosot.javademo.mysql;

import com.geosot.javademo.units.MySQLConnect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ReadPointCloudFromMysql {
    public static void main(String[] args) {
        try {
            Connection connection = MySQLConnect.getConnection();
            String query = "SELECT * FROM las_0 WHERE code >= ? AND code <= ?";
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, "0004836404CA6C92D2410442");
            preparedStatement.setString(2, "0004836404D82CB012289282");

            // 执行查询
            long start = System.currentTimeMillis();
            ResultSet resultSet = preparedStatement.executeQuery();
//            int count = 0;
            int batchSize = 1000;
//            // 查询结果
            while (resultSet.next()) {
//                // 打印查询结果
//                double x = resultSet.getDouble("x");
//                double y = resultSet.getDouble("y");
//                double z = resultSet.getDouble("z");
//                count++;
//                System.out.println("x: " + x+", y: " + y+", z: " + z);

                if (--batchSize == 0) {
                    batchSize = 1000;
                    resultSet = preparedStatement.executeQuery(); // 获取下一批数据
                }
            }
            System.out.println((System.currentTimeMillis()-start)+"ms");
//            System.out.println(count);
            // 关闭资源
            resultSet.close();
            preparedStatement.close();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
