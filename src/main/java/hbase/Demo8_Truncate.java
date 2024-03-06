package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class Demo8_Truncate {
    public static void main(String[] args) {
        try {
            //构建配置对象：自动读取hbase-site.xml
            Configuration conf = HBaseConfiguration.create();
            //构建Connection对象
            Connection connection = ConnectionFactory.createConnection(conf);
            //构建Admin对象，用于DDL操作
            Admin admin = connection.getAdmin();
            //定义表名
            String tableName = "student";
            //构建TableName对象
            TableName tn = TableName.valueOf(tableName);
            //判断表是否存在，不存在，则无法添加数据
            if (!admin.tableExists(tn)) {
                //提示信息
                System.out.printf("表 %s 不存在~~~%n", tableName);
                return;
            }
            //清空数据
            admin.truncateTable(tn, false);
            //提示
            System.out.println("表中数据清除成功~~~");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
