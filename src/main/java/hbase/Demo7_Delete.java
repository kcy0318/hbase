package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Demo7_Delete {
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
            byte[] rk = Bytes.toBytes("1001");
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            //前置：判断行是否存在
            Get get = new Get(rk);
            if (!studentTable.exists(get)) {
                System.out.printf("1001的数据不存在");
                return;
            }
            //删除1（删除整行）：构建Delete对象
            //Delete delete = new Delete(rk);
            //删除2（删除某一行中指定列簇，若只有一个列簇，就是删除整行）：构建Delete对象
            //Delete delete = new Delete(rk);
            //delete.addFamily(fm);
            //删除3（删除指定行，指定列簇，指定列名的单元格的值）：构建Delete对象
            //Delete delete = new Delete(rk);
            //delete.addColumn(fm, c1);
            //删除4（删除指定行，指定列簇，指定列名,指定版本号的单元格的值）：构建Delete对象
            Delete delete = new Delete(rk);
            delete.addColumn(fm, c1, 12341245234L);
            //执行删除操作
            studentTable.delete((delete));
            //提示
            System.out.printf("数据删除成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
