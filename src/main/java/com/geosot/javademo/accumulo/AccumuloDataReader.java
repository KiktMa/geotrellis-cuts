package com.geosot.javademo.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.Map;

public class AccumuloDataReader {
    /**
     * 根据GeoSot-3D编码对数据进行范围插叙
     * 可以根据两个坐标点来生成编码来对数据进行动态查询
     * @param args
     */
    public static void main(String[] args) {

        try {
            Connector conn = AccumuloDataUploader.getConn();
            Scanner scanner = conn.createScanner("large_point_cloud", Authorizations.EMPTY);
            long start = System.currentTimeMillis();
            // 48  15
            scanner.setRange(new Range("0004836404CA6C92D2410442","0004836404D82CB012289282"));
            int count = 0;
//            for (Map.Entry<Key, Value> pointcloud : scanner) {
//                Text columnx = pointcloud.getKey().getColumnQualifier();
//                Text columnx = pointcloud.getKey().getColumnFamily(new Text("x"));
//                Text columny = pointcloud.getKey().getColumnFamily(new Text("y"));
//                Text columnz = pointcloud.getKey().getColumnFamily(new Text("z"));
//                Value pointcloudValue = pointcloud.getValue();
//                count++;
//                System.out.println("columnx:"+columnx+/*", columnx:"+columny+", columnz:"+columnz+*/", pointcloudValue:"+pointcloudValue);
//            }
            System.out.println((System.currentTimeMillis()-start)+"ms");
//            System.out.println(count);
        }catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}

