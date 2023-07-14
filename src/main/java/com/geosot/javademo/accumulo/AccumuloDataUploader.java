package com.geosot.javademo.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class AccumuloDataUploader {

    private static final String instanceName = "accumulo";
    private static final String zooServers = "node1:2181,node2:2181,node3:2181";
    public static final String username = "root";
    public static final PasswordToken passworld = new PasswordToken("root");
    private static final String TABLE_NAME = "point_cloud";
    private static final String COLUMN_FAMILY = "information";
    private static final String[] COLUMN_QUALIFIERS = {"x", "y", "z", "lon", "lat"};

    private final Connector connector;

    public AccumuloDataUploader(Connector connector) {
        this.connector = connector;
    }

    public void createTable() throws AccumuloException, AccumuloSecurityException, TableExistsException {
        if (!connector.tableOperations().exists(TABLE_NAME)) {
            connector.tableOperations().create(TABLE_NAME);
        }
    }

    public void uploadData(String filePath) throws IOException, TableNotFoundException, MutationsRejectedException {
        BatchWriterConfig writerConfig = new BatchWriterConfig();
        BatchWriter batchWriter = connector.createBatchWriter(TABLE_NAME, writerConfig);

        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line;
        while ((line = reader.readLine()) != null) {
            Mutation mutation = parseLineToMutation(line);
            batchWriter.addMutation(mutation);
        }

        batchWriter.close();
        reader.close();
    }

    private Mutation parseLineToMutation(String line) {
        String[] parts = line.split(" ");
        String rowId = parts[8];
        Mutation mutation = new Mutation(rowId);

        for (int i = 0; i < COLUMN_QUALIFIERS.length; i++) {
            String columnQualifier = COLUMN_QUALIFIERS[i];
            String value = parts[i];

            mutation.put(new Text(COLUMN_FAMILY), new Text(columnQualifier), new Value(value.getBytes()));
        }

        return mutation;
    }

    public static Connector getConn() throws AccumuloException, AccumuloSecurityException {
        ZooKeeperInstance zooKeeperInstance = new ZooKeeperInstance(instanceName, zooServers);
        Connector conn = zooKeeperInstance.getConnector(username, passworld);
        return conn;
    }

    public static void main(String[] args) {

        try {
            Connector connector = getConn();
            AccumuloDataUploader dataUploader = new AccumuloDataUploader(connector);

            // 若表不存在则创建
            dataUploader.createTable();

            // 加载文件
            String filePath = "C:\\Users\\mj\\Desktop\\paper\\pointcloud\\corrected-LJYY-Cloud-1-0-9 - Cloud - tree.txt\\part-00000-3da05387-1bc8-4af6-8570-ba58598adb79-c000.txt";
            dataUploader.uploadData(filePath);

            System.out.println("点云数据存储成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}