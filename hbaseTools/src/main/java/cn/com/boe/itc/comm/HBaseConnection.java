package cn.com.boe.itc.comm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnection {

    private Configuration conf;
    private Connection conn;
    private Admin admin; //Hbase中所有对表的管理操作都需要admin对象

    public Connection getConnection() throws Exception {
        conf = HBaseConfiguration.create();
        conf.set("hbase.master","10.141.68.11");
        conf.set("hbase.zookeeper.quorum","10.141.68.10,10.141.68.11,10.141.68.23");
//        conf.set("hbase.rootdir","file:///opt/hbase_data");
//        conf.set("hbase.zookeeper.property.dataDir","/opt/hbase_data/zookeeper");
        try {
            //2.连接HBase,定义conn对象
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }


    /**
     * 关闭HBase数据库连接
     */
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (admin != null) {
            admin.close();
        }
    }
}
