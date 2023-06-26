package com.spark.demo.connect

import java.sql.{Connection, DriverManager, ResultSet}

class MetadataWrite {

}

object MetadataWrite{
  val conn_str = "jdbc:postgresql://192.168.2.12:5432/"

  val conn: Connection = DriverManager.getConnection(conn_str, "postgres", "postgres")

  try {
    // Configure to be Read Only
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    // Execute Query
    val rs = statement.executeQuery("SELECT * FROM spark_table")
    // Iterate Over ResultSet
    while (rs.next) {
      println(rs.getString("xxx"))
    }
  }
  finally {
    conn.close
  }
}