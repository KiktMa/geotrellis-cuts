package com.geosot.javademo.geosot;

import com.esotericsoftware.kryo.io.Input;
import geotrellis.raster.Tile;
import geotrellis.spark.io.accumulo.AccumuloAttributeStore;
import geotrellis.spark.io.accumulo.AccumuloLayerReader;
import geotrellis.spark.io.accumulo.AccumuloValueReader;
import geotrellis.vector.Extent;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import com.esotericsoftware.kryo.Kryo;
import java.io.ByteArrayInputStream;

import org.apache.spark.util.ByteBufferInputStream;
import org.apache.spark.util.io.ChunkedByteBufferInputStream;

import javax.imageio.stream.FileImageOutputStream;
import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static org.apache.xmpbox.type.Cardinality.Seq;

public class ScanTiff {
    public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        String instanceName = "accumulo";
        String zooServers = "node1:2181,node2:2181,node3:2181";
        KryoSerializer kryoSerializer = new KryoSerializer(new SparkConf(true));
        Kryo kryo = new Kryo();
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
            Tile tile = (Tile) kryo.readClassAndObject(new Input(bytes));
            long timestamp = entry.getKey().getTimestamp();
//            entry.getKey().set(new Key("id"));
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
