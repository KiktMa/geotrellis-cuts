package com.geosot.javademo.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class AccumuloDataReader {
    /**
     * 根据GeoSot-3D编码对数据进行范围插叙
     * 可以根据两个坐标点来生成编码来对数据进行动态查询
     */
    public List<Map<String, Object>> queryPointCloud() {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connector conn = AccumuloDataUploader.getConn();
            Scanner scanner = conn.createScanner("large_point_cloud", Authorizations.EMPTY);
//            long start = System.currentTimeMillis();
            // 48  15
            scanner.setRange(new Range("0004836404CA6C929B659659","0004836404CA6C96890C9008"));
//            int count = 0;
            for (Map.Entry<Key, Value> pointcloud : scanner) {
                Map<String, Object> point = new HashMap<>();
                point.put(pointcloud.getKey().toString(), pointcloud.getValue().get());
//                point.put(pointcloud.getKey().getColumnFamily(new Text("y")).toString(), pointcloud.getValue());
//                point.put(pointcloud.getKey().getColumnFamily(new Text("z")).toString(), pointcloud.getValue());
                result.add(point);
            }
//            System.out.println((System.currentTimeMillis()-start)+"ms");
//            System.out.println(count);
        }catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}

