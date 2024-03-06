package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class Demo3_MorePut {
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
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            byte[] c2 = Bytes.toBytes("age");
            byte[] c3 = Bytes.toBytes("address");
            byte[] c4 = Bytes.toBytes("score");
            //构造多行
            List<Put> putList = new ArrayList<Put>();
            //记录开始时间
            long beginTime = System.currentTimeMillis();
            //循环添加数据
            for (int i = 1; i < 1000000; i++) {
                //定义行键
                byte[] rk = Bytes.toBytes(String.valueOf(i));
                byte[] v1 = Bytes.toBytes("zhangsan");
                byte[] v2 = Bytes.toBytes("18");
                byte[] v3 = Bytes.toBytes("nanjing");
                byte[] v4 = Bytes.toBytes("65.5");
                //构建Put对象
                Put put = new Put(rk);
                put.addColumn(fm, c1, v1);
                put.addColumn(fm, c2, v2);
                put.addColumn(fm, c3, v3);
                put.addColumn(fm, c4, v4);
                //添加多行
                putList.add(put);
                //判断1000行时，增加一次
                if (i % 1000 == 0) {
                    //执行增加数据操作
                    studentTable.put(putList);
                    //清空集合
                    putList.clear();
                }
            }
            //判断集合中是否还有行
            if (putList.size() > 0) {
                //执行增加数据操作
                studentTable.put(putList);
                //清空集合
                putList.clear();
            }
            //记录结束时间
            long endTime = System.currentTimeMillis();
            //提示信息
            System.out.printf("百万数据添加成功，总时间：%d 秒", (endTime-beginTime) / 1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
