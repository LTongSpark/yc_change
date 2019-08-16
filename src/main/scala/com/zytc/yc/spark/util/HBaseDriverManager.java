package com.zytc.yc.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseDriverManager {

    public static Configuration conf = null;
    private static Connection conn;

    public static Configuration getHbaseConf() {
        return conf;
    }

    static {
        conf = HBaseConfiguration.create();
//        conf.set("hbase.rootdir","hdfs://mycluster/hbase");
//        conf.set("hbase.zookeeper.quorum","s102:2181,s103:2181,s104:2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    public static Connection getConnection() {
        return conn;
    }
    public static synchronized void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
