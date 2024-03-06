package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Demo1_CreateTable {
    public static void main(String[] args) {
        try {
            //构建配置对象：自动读取hbase-site.xml
            Configuration conf = HBaseConfiguration.create();
            //手动配置属性
            //conf.set("hbase.zookeeper.property.client", "2181");
            //conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
            //构建Connection对象
            Connection connection = ConnectionFactory.createConnection(conf);
            //构建Admin对象，用于DDL操作
            Admin admin = connection.getAdmin();
            //定义表名
            String tableName = "student";
            //构建TableName对象
            TableName tn = TableName.valueOf(tableName);
            //判断表是否存在，存在则删除之
            if (admin.tableExists(tn)) {
                //先禁用表
                admin.disableTable(tn);
                //再删除表
                admin.deleteTable(tn);
                //提示信息
                System.out.printf("表 %s 已存在，删除成功(●ˇ∀ˇ●)%n", tableName);
            }
            //定义列簇结构对象
            String familyName = "info";
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
