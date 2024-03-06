package edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtils {
    private static Configuration conf;
    private static Connection connection;
    private static Admin admin;
    public static void init() {
        try {
            //构建配置对象：自动读取hbase-site.xml
            conf = HBaseConfiguration.create();
            //手动配置属性
            //conf.set("hbase.zookeeper.property.client", "2181");
            //conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
            //构建Connection对象
            connection = ConnectionFactory.createConnection(conf);
            //构建Admin对象，用于DDL操作
            admin = connection.getAdmin();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    static {
        init();
    }
    public static TableName getTableName(String tableName) {
        TableName tn  = TableName.valueOf(tableName);
        return tn;
    }
    public static Table getTable(String tableName) {
        Table table = null;
        try {
            TableName tn = getTableName(tableName);
            if (admin.tableExists(tn)) {
                table = connection.getTable(tn);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return table;
    }
    public static void createTable(String tableName, String familyName) {
        try {
            TableName tn = getTableName(tableName);
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
                System.out.printf("表 %s 存在，已删除%n", tableName);
            }
            byte[] fm = Bytes.toBytes(familyName);
            ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(fm);
            ColumnFamilyDescriptor cfd = cfdb.build();
            //构建表结构对象
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
            tdb.setColumnFamily(cfd);
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
            //提示信息
            System.out.printf("表 %s 创建成功，删除成功(●ˇ∀ˇ●)%n", tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Configuration getConf() {
        return conf;
    }
    public static Connection getConnection() {
        return connection;
    }
    public static Admin getAdmin() {
        return admin;
    }
}
