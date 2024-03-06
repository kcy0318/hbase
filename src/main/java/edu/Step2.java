package edu;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Scanner;

public class Step2 {
    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);
            System.out.print("请输入学号：");
            String id = scanner.next();
            String tableName = "score";
            //构建Table对象
            Table scoreTable = HbaseUtils.getTable(tableName);
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
            //定义过滤器
            byte[] rk = Bytes.toBytes(id);
            Filter filter = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(rk));
            //设置过滤器
            scan.setFilter(filter);
            //执行读取操作
            ResultScanner rows = scoreTable.getScanner(scan);
            //读取值
            String name = "", classId ="", examDate="";
            int chinese = 0, math = 0, english = 0;
            for (Result row : rows) {
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
                System.out.printf("%s\t%s\t%s\t%s\t%d\t%d\t%d%n", id, name, classId, examDate, chinese, math, english);
                //输出总分
                System.out.printf("总分：%d%n", chinese + math + english);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
