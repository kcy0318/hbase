package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Demo6_Scan {
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
            //构建Table对象
            Table studentTable = connection.getTable(tn);
            //定义行键、列簇、列名
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            byte[] c2 = Bytes.toBytes("age");
            byte[] c3 = Bytes.toBytes("address");
            byte[] c4 = Bytes.toBytes("score");
            //构建Scan对象
            Scan scan = new Scan();
            //执行读取操作
            ResultScanner rows = studentTable.getScanner(scan);
            //读取值
            String no = "";
            String name = "";
            int age = 0;
            String address = "";
            double score = 0;
            for (Result row : rows) {
                //读取行键
                no = Bytes.toString(row.getRow());
                //读取指定列簇、列名的单元格的值
                if (row.containsColumn(fm, c1)) {
                    name = Bytes.toString(row.getValue(fm, c1));
                }
                if (row.containsColumn(fm, c2)) {
                    age = Bytes.toInt(row.getValue(fm, c2));
                }
                if (row.containsColumn(fm, c3)) {
                    address = Bytes.toString(row.getValue(fm, c3));
                }
                if (row.containsColumn(fm, c4)) {
                    score = Bytes.toDouble(row.getValue(fm, c4));
                }
                //输出整行数据
                System.out.printf("%s\t%s\t%d\t%s\t%f%n", no, name, age, address, score);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
