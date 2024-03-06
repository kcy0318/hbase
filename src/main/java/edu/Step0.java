package edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Step0 {
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
            String tableName = "score";
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
            //定义行键、列簇、列名
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            byte[] c2 = Bytes.toBytes("classId");
            byte[] c3 = Bytes.toBytes("examDate");
            byte[] c4 = Bytes.toBytes("chinese");
            byte[] c5 = Bytes.toBytes("math");
            byte[] c6 = Bytes.toBytes("english");
            //构建Scan对象
            Scan scan = new Scan();
            //执行读取操作
            ResultScanner rows = studentTable.getScanner(scan);
            //读取值
            String no="", name = "", classId ="", examDate="";
            int chinese = 0, math = 0, english = 0;
            for (Result row : rows) {
                //读取行键
                no = Bytes.toString(row.getRow());
                //读取指定列簇、列名的单元格的值
                if (row.containsColumn(fm, c1)) {
                    name = Bytes.toString(row.getValue(fm, c1));
                }
                if (row.containsColumn(fm, c2)) {
                    classId = Bytes.toString(row.getValue(fm, c2));
                }
                if (row.containsColumn(fm, c3)) {
                    examDate = Bytes.toString(row.getValue(fm, c3));
                }
                if (row.containsColumn(fm, c4)) {
                    chinese = Bytes.toInt(row.getValue(fm, c4));
                }
                if (row.containsColumn(fm, c5)) {
                    math = Bytes.toInt(row.getValue(fm, c5));
                }
                if (row.containsColumn(fm, c6)) {
                    english = Bytes.toInt(row.getValue(fm, c6));
                }
                //输出整行数据
                System.out.printf("%s\t%s\t%s\t%s\t%d\t%d\t%d%n", no, name, classId, examDate, chinese, math, english);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
