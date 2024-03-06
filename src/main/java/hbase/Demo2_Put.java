package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Demo2_Put {
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
            //判断表是否存在，不存在，则无法添加数据
            if (!admin.tableExists(tn)) {
                //提示信息
                System.out.printf("表 %s 不存在~~~%n", tableName);
                return;
            }
            //构建Table对象
            Table studentTable = connection.getTable(tn);
            //定义列簇、列名
            byte[] rk1 = Bytes.toBytes("1001");
            byte[] rk2 = Bytes.toBytes("1002");
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            byte[] c2 = Bytes.toBytes("age");
            byte[] c3 = Bytes.toBytes("address");
            byte[] c4 = Bytes.toBytes("score");
            byte[] v1 = Bytes.toBytes("zhangsan");
            byte[] v2 = Bytes.toBytes(18);
            byte[] v3 = Bytes.toBytes("nanjing");
            byte[] v4 = Bytes.toBytes(65.5);
            byte[] v5 = Bytes.toBytes("lisi");
            byte[] v6 = Bytes.toBytes(22);
            byte[] v7 = Bytes.toBytes("beijing");
            byte[] v8 = Bytes.toBytes(85.5);
            //构建Put对象
            Put put1 = new Put(rk1);
            put1.addColumn(fm, c1, v1);
            put1.addColumn(fm, c2, v2);
            put1.addColumn(fm, c3, v3);
            put1.addColumn(fm, c4, v4);
            //执行增加数据操作
            studentTable.put(put1);
            //构建Put对象
            Put put2 = new Put(rk2);
            put2.addColumn(fm, c1, v5);
            put2.addColumn(fm, c2, v6);
            put2.addColumn(fm, c3, v7);
            put2.addColumn(fm, c4, v8);
            //执行增加数据操作
            studentTable.put(put2);
            //提示信息
            System.out.printf("数据添加成功*^____^*");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
