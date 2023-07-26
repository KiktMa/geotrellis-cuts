package com.geosot.javademo.units;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLConnect {

    private static final String host ="localhost";
    private static final String port = "3306";
    private static final String tablename ="pointcloud";
    private static final String username ="root";
    private static final String password = "root";

    private MySQLConnect(){}

    private static class HolderClass{
        private static Connection connection = null;
        static {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection(
                        "jdbc:mysql://"+host+":"+port+"/"+tablename+"?useSSL=false",
                        username,
                        password);
                connection.setAutoCommit(false);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Connection getConnection(){
        return HolderClass.connection;
    }
}
