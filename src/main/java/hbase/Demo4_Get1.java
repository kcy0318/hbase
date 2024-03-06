package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Demo4_Get1 {
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
            //定义行键
            byte[] rk = Bytes.toBytes("1001");
            //构建Get对象
            Get get = new Get(rk);
            //执行读取操作
            Result row = studentTable.get(get);
            //遍历每一个单元值
            System.out.printf("%s\t%s\t%s\t%s%n","姓名","年龄","地址","成绩");
            for (Cell cell : row.rawCells()) {
                //读取行键： 其实不需要读取的，因为你就是根据行键读取的
                String no = Bytes.toString(CellUtil.cloneRow(cell));
                //读取列簇
                String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
                //读取列名
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                //读取单元格的值
                if ("info".equals(familyName) && "name".equals(columnName)) {
                    String name = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.printf("%s\t", name);
                } else if ("info".equals(familyName) && "age".equals(columnName)) {
                    int age = Bytes.toInt(CellUtil.cloneValue(cell));
                    System.out.printf("%d\t", age);
                } else if ("info".equals(familyName) && "address".equals(columnName)) {
                    String address = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.printf("%s\t", address);
                } else if ("info".equals(familyName) && "score".equals(columnName)) {
                    double score = Bytes.toDouble(CellUtil.cloneValue(cell));
                    System.out.printf("%f%n", score);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
