package com.cs523;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseWriter {
    private Table table;
    private Connection connection;
    TableUtils tableUtils = new TableUtils();

    public HBaseWriter() throws IOException {
        connection = tableUtils.newConnection();
    }

    void writeEvents(List<Tuple2<String, Integer>> events, String time) throws IOException {
        table = connection.getTable(TableUtils.TABLE);
        Put p = new Put(Bytes.toBytes(time));
        for (Tuple2<String, Integer> event : events) {
            p.addColumn(TableUtils.CF_EVENTS.getBytes(), b(event._1()), b(event._2().toString()));
        }
        table.put(p);
        close();
    }

    void writeBrandReport(List<Tuple2<String, Long>> rows, String col) throws IOException {
        table = connection.getTable(TableUtils.TABLE_ANALYTICS);
        List<Put> puts = new ArrayList<>();
        for (Tuple2<String, Long> row : rows) {
            Put p = new Put(b(row._1()));
            p.addColumn(TableUtils.CF_REPORT.getBytes(), b(col), Bytes.toBytes(row._2().toString()));
            puts.add(p);
        }
        table.put(puts);
        close();
    }

    public void close() throws IOException {
        table.close();
    }

    public byte[] b(String v) {
        return Bytes.toBytes(v);
    }
}
