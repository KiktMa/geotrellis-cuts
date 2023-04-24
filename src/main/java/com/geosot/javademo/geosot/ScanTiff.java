package com.geosot.javademo.geosot;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import javax.imageio.stream.FileImageOutputStream;
import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

public class ScanTiff {
    public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        String instanceName = "accumulo";
        String zooServers = "node1:2181,node2:2181,node3:2181";
        Instance inst = new ZooKeeperInstance(instanceName, zooServers);
        Connector conn = inst.getConnector("root", "root");

        Scanner scan = conn.createScanner("slope",new Authorizations());
//        Range range = new Range();
//        scan.setRange(range);
//        scan.fetchColumn(new Text("layer_slope"),new Text("11"));
//        Authorizations authorizations = scan.getAuthorizations();
//        System.out.println(authorizations);
        int i = 0;
        for(Map.Entry<Key, Value> entry : scan) {
//            String row = String.valueOf(entry.getKey().getRow());
            String columnFamily = String.valueOf(entry.getKey().getColumnFamily());
            Value value = entry.getValue();
            byte[] bytes = value.get();
            long timestamp = entry.getKey().getTimestamp();
            LocalDateTime localDateTime1 = Instant.ofEpochSecond(timestamp).atOffset(ZoneOffset.UTC).toLocalDateTime();
            OffsetDateTime dateTime1 = OffsetDateTime.of(localDateTime1, ZoneOffset.UTC);
            System.out.println(dateTime1);
//            i++;
//            byte2image(bytes,"D:\\test_tif\\18level\\test"+i+".tif");
        }
    }

    public static void byte2image(byte[] data, String path) {
        if (data.length < 3 || path.equals("")) return;
        try {
            FileImageOutputStream imageOutput = new FileImageOutputStream(new File(path));
            imageOutput.write(data, 0, data.length);
            imageOutput.close();
            System.out.println("Make Picture success,Please find image in " + path);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex);
            ex.printStackTrace();
        }
    }
}
