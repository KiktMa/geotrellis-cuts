package com.geosot.javademo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class AccumuloRasterTiles {
    private static final String TABLE_NAME = "raster_tiles";
    private static final String[] LEVELS = { "level_0", "level_1", "level_2" };
    private static final PasswordToken password = new PasswordToken("root");
    private static final String user = "root";
    private static final String instance = "accumulo";
    private static final String zookeepers = "node1:2181,node2:2181,node3:2181";
    private static final String table = "test";

    public static void main(String[] args) throws TableExistsException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        // 获取Accumulo连接
        ZooKeeperInstance inst = new ZooKeeperInstance(instance, zookeepers);
        Connector connector = inst.getConnector("root", password);

        // 创建表
        try {
            connector.tableOperations().create(TABLE_NAME);
        } catch (TableExistsException e) {
            // 表已经存在，忽略异常
            e.printStackTrace();
        }

        // 为每个列族创建一个BatchWriter
        BatchWriter[] writers = new BatchWriter[LEVELS.length];
        for (int i = 0; i < LEVELS.length; i++) {
            writers[i] = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
            writers[i].addMutation(new Mutation(new Text(LEVELS[i])));
        }

        // 向表中添加示例数据
//        for (int level = 0; level < LEVELS.length; level++) {
//            int tileSize = 256 / (1 << level); // 计算瓦片大小
//            for (int x = 0; x < (1 << level); x++) {
//                for (int y = 0; y < (1 << level); y++) {
//                    byte[] tileData = "";// 从文件中读取瓦片数据并序列化成二进制格式
//                    Mutation mutation = new Mutation(new Text(LEVELS[level]));
//                    mutation.put(new Text(x + "_" + y), new Text(""), new Value(tileData));
//                    writers[level].addMutation(mutation);
//                }
//                writers[level].flush();
//            }
//        }
//        // 关闭BatchWriter和Accumulo连接
//        for (int i = 0; i < LEVELS.length; i++) {
//            writers[i].close();
//        }
    }
}
