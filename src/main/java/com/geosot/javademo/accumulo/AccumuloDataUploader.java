package com.geosot.javademo.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class AccumuloDataUploader {

    private static final String instanceName = "accumulo";
    private static final String zooServers = "node1:2181,node2:2181,node3:2181";
    public static final String username = "root";
    public static final PasswordToken passworld = new PasswordToken("root");
    private static final String TABLE_NAME = "large_point_cloud";
    private static final String COLUMN_FAMILY = "information";
    private static final String[] COLUMN_QUALIFIERS = {"x", "y", "z", "lon", "lat"};

    private final Connector connector;

    public AccumuloDataUploader(Connector connector) {
        this.connector = connector;
    }

    /**
     * 创建Accumulo数据库表格
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     */
    public void createTable() throws AccumuloException, AccumuloSecurityException, TableExistsException {
        if (!connector.tableOperations().exists(TABLE_NAME)) {
            connector.tableOperations().create(TABLE_NAME);
        }
    }

    /**
     * 批量读取点云数据并批量上传到accumulo数据库中
     * @param filePath 点云文件存储路径
     * @throws IOException
     * @throws TableNotFoundException
     * @throws MutationsRejectedException
     */
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

    /**
     * 为将点云数据存储到accumulo相应的列中
     * @param line 将文件按照一行一行读取，因为点云文件的一行代表一个点数据
     * @return
     */
    private Mutation parseLineToMutation(String line) {
        String[] parts = line.split(" ");
        String rowId = parts[5];
        Mutation mutation = new Mutation(rowId);

        for (int i = 0; i < COLUMN_QUALIFIERS.length; i++) {
            String columnQualifier = COLUMN_QUALIFIERS[i];
            String value = parts[i];

            mutation.put(new Text(COLUMN_FAMILY), new Text(columnQualifier), new Value(value.getBytes()));
        }

        return mutation;
    }

    /**
     * 创建Accumulo连接，通过zookeeper来连接
     * @return
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    public static Connector getConn() throws AccumuloException, AccumuloSecurityException {
        ZooKeeperInstance zooKeeperInstance = new ZooKeeperInstance(instanceName, zooServers);
        Connector conn = zooKeeperInstance.getConnector(username, passworld);
        return conn;
    }

    /**
     * 主函数，启动加载持久化点云数据
     * @param args
     */
    public static void main(String[] args) {

        try {
            Connector connector = getConn();
            AccumuloDataUploader dataUploader = new AccumuloDataUploader(connector);

            // 若表不存在则创建
            dataUploader.createTable();

            // 加载文件
            String filePath = "D:\\JavaConsist\\MapData\\180m_pointcloud\\0\\new\\part-all-clean.txt";

            long startTime = System.currentTimeMillis();
            dataUploader.uploadData(filePath);

            System.out.println((System.currentTimeMillis()-startTime)+"ms");
            System.out.println("点云数据存储成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}