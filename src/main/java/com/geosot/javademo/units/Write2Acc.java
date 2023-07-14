package com.geosot.javademo.units;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.Collections;
import java.util.Map;

public class Write2Acc {
    public static void main(String[] args) throws Exception {
        Connector connector = new ZooKeeperInstance("accumulo", "node1:2181,node2:2181,node3:2181")
                .getConnector("root", new PasswordToken("root"));
        BatchScanner scanner = connector.createBatchScanner("test1", Authorizations.EMPTY, 10);
        scanner.setRanges(Collections.singleton(new Range()));
        scanner.fetchColumnFamily(new Text("colFamily"));

        BatchWriter writer = connector.createBatchWriter("tableName", new BatchWriterConfig());

        for (Map.Entry<Key, Value> entry : scanner) {
            Mutation mutation = new Mutation(entry.getKey().getRow());

            mutation.put(new Text("uniqueCodeColFamily"), new Text("uniqueCodeQualifier"), new Value("uniqueCode".getBytes()));

            writer.addMutation(mutation);
        }

        writer.close();
        scanner.close();
    }
}
