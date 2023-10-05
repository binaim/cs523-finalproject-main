package com.cs523;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class TableUtils {
    public static TableName TABLE = TableName.valueOf("electronic-store");
    public static TableName TABLE_ANALYTICS = TableName.valueOf("electronic-analytics");
    public static String CF_EVENTS = "events";
    public static String CF_REPORT = "report";

    public Configuration getConfig() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        return config;
    }

    public Connection newConnection() throws IOException {
        return ConnectionFactory.createConnection(getConfig());
    }

    public void createTable() throws IOException {
        // Create a connection to the HBase server
        Connection connection = newConnection();
        // Create an admin object to perform table administration operations
        Admin admin = connection.getAdmin();

        // Create a table descriptor
        TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TABLE)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_EVENTS))
                .build();

        // Create the table
        if (admin.tableExists(TABLE)) {
            admin.disableTable(TABLE);
            admin.deleteTable(TABLE);
        }
        admin.createTable(tableDesc);
        System.out.println("==== Table created!!! ====");
    }

    public void createAnalyticsTable() throws IOException {
        // Create a connection to the HBase server
        Connection connection = newConnection();
        // Create an admin object to perform table administration operations
        Admin admin = connection.getAdmin();

        // Create a table descriptor
        TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TABLE_ANALYTICS)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_REPORT))
                .build();

        // Create the table
        if (admin.tableExists(TABLE_ANALYTICS)) {
            admin.disableTable(TABLE_ANALYTICS);
            admin.deleteTable(TABLE_ANALYTICS);
        }
        admin.createTable(tableDesc);
        System.out.println("==== Table analytics created!!! ====");
    }
}
