package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Demo10_Filter {
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
            //定义行键、列簇、列名
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            byte[] c2 = Bytes.toBytes("age");
            byte[] c3 = Bytes.toBytes("address");
            byte[] c4 = Bytes.toBytes("score");
            //构建Scan对象
            Scan scan = new Scan();
            //定义过滤器
            //行键过滤器
            //1:
            //byte[] rk = Bytes.toBytes("1001");
            //Filter filter = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(rk));
            //2:
            //byte[] rk = Bytes.toBytes("10");
            //Filter filter = new RowFilter(CompareOperator.EQUAL, new BinaryPrefixComparator(rk));
            //3:
            //String rowKey = "00";
            //Filter filter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rowKey));
            //4:
            //String rowKey = "^\\d{4}$";
            //Filter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(rowKey));
            //行键前缀过滤器:
            //byte[] rk = Bytes.toBytes("10");
            //Filter filter = new PrefixFilter(rk);
            //只返回行键的过滤器
            //Filter filter = new KeyOnlyFilter();
            //列簇过滤器，返回包含该列簇的行的数据
            //1:
            //byte[] f = Bytes.toBytes("info");
            //Filter filter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(f));
            //2:
            //byte[] f = Bytes.toBytes("in");
            //Filter filter = new FamilyFilter(CompareOperator.EQUAL, new BinaryPrefixComparator(f));
            //3:
            //String f = "nf";
            //Filter filter = new FamilyFilter(CompareOperator.EQUAL, new SubstringComparator(f));
            //4:
            //String f = "^[a-zA-z]{4}$";
            //Filter filter = new FamilyFilter(CompareOperator.EQUAL, new RegexStringComparator(f));
            //列名过滤器，返回列和行键值
            //1:
            //[] c = Bytes.toBytes("age");
            //Filter filter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(c));
            //2:
            //byte[] c = Bytes.toBytes("a");
            //Filter filter = new QualifierFilter(CompareOperator.EQUAL, new BinaryPrefixComparator(c));
            //3:
            //String c = "a";
            //Filter filter = new QualifierFilter(CompareOperator.EQUAL, new SubstringComparator(c));
            //4:
            //String c = "e$";
            //Filter filter = new QualifierFilter(CompareOperator.EQUAL, new RegexStringComparator(c));
            //值过滤器，只返回符合条件的列和行键值
            //1:
            //byte[] v = Bytes.toBytes("zhangsan");
            //Filter filter = new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(v));
            //2:
            //byte[] v = Bytes.toBytes("z");
            //Filter filter = new ValueFilter(CompareOperator.EQUAL, new BinaryPrefixComparator(v));
            //3:
            //String v = "a";
            //Filter filter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator(v));
            //4:
            //String v = "n";
            //Filter filter = new ValueFilter(CompareOperator.EQUAL, new RegexStringComparator(v));
            //根据指定列簇、列名和值来决定行是否被过滤
            //byte[] v = Bytes.toBytes("lisi");
            //Filter filter = new SingleColumnValueFilter(fm, c1, CompareOperator.EQUAL, v);
            //根据指定列簇、列名和值，只返回符合条件的列和行键值，其它列簇和列不返回
            //byte[] v = Bytes.toBytes("lisi");
            //Filter filter = new ColumnValueFilter(fm, c1, CompareOperator.EQUAL, v);
            //RandomRowFilter 随机返回行键，其他列簇和列并不返回
            //Filter filter = new RandomRowFilter(1f);
            //组合过滤器
            //定义过滤器1
            String c = "e$";
            Filter f1 = new QualifierFilter(CompareOperator.EQUAL, new SubstringComparator(c));
            //定义过滤器2
            String v = "n";
            Filter f2 = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator(v));
            //组合过滤器
            //Filter filter = new FilterList(FilterList.Operator.MUST_PASS_ALL, f1, f2);
            Filter filter = new FilterList(FilterList.Operator.MUST_PASS_ONE, f1, f2);
            //设置过滤器
            scan.setFilter(filter);
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
