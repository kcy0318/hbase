package edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step5 {
    public static class MyMapper extends TableMapper<Text, IntWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            byte[] c2 = Bytes.toBytes("classId");
            byte[] c3 = Bytes.toBytes("examDate");
            byte[] c4 = Bytes.toBytes("chinese");
            byte[] c5 = Bytes.toBytes("math");
            byte[] c6 = Bytes.toBytes("english");
            if (value.containsColumn(fm, c1)) {
                //读取指定列的值
                String line0 = Bytes.toString(value.getRow());
                String line1 = Bytes.toString(value.getValue(fm, c1));
                String line2 = Bytes.toString(value.getValue(fm, c2));
                int line4 = Bytes.toInt(value.getValue(fm, c4));
                int line5 = Bytes.toInt(value.getValue(fm, c5));
                int line6 = Bytes.toInt(value.getValue(fm, c6));
                int total = 300 - (line4 + line5 + line6);
                String result = total + "";
                if (total < 10) {
                    result = "00" + total;
                } else if (total < 100) {
                    result = "0" + total;
                }
                //输出
                context.write(new Text(result + "_" + line0 + " " + line2 + " " + line1), new IntWritable(line4 + line5 + line6));
            }
        }
    }
    public static class MyReducer extends TableReducer<Text, IntWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            int score = 0;
            for (IntWritable v : values) {
                score = v.get();
            }
            //定义行键、列键、列名、值
            //020_10336 3 褚文蕾
            byte[] rk = Bytes.toBytes(key.toString());
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("id");
            byte[] c2 = Bytes.toBytes("classId");
            byte[] c3 = Bytes.toBytes("name");
            byte[] c4 = Bytes.toBytes("score");
            byte[] v1 = Bytes.toBytes(key.toString().substring(4, 9));
            byte[] v2 = Bytes.toBytes(key.toString().substring(10, 11));
            byte[] v3 = Bytes.toBytes(key.toString().substring(12));
            byte[] v4 = Bytes.toBytes(score);
            //构建Put
            Put put = new Put(rk);
            put.addColumn(fm, c1, v1);
            put.addColumn(fm, c2, v2);
            put.addColumn(fm, c3, v3);
            put.addColumn(fm, c4, v4);
            //输出
            context.write(NullWritable.get(), put);
        }
    }
    public static void main(String[] args) {
        try {
            //构建配置对象：自动读取hbase-site.xml
            Configuration conf = HbaseUtils.getConf();
            //构建Connection对象
            Connection connection = ConnectionFactory.createConnection(conf);
            //构建Admin对象，用于DDL操作
            Admin admin = connection.getAdmin();
            //定义表名
            String tableName = "scoreOFGrade";
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
            System.out.printf("表 %s 创建成功(●ˇ∀ˇ●)%n", tableName);
            //设置输出表
            conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
            //创建job任务
            Job job = Job.getInstance(conf, "grade count");
            //Scan 读取源表中的数据
            Scan scan = new Scan();
            //设置mapper
            TableMapReduceUtil.initTableMapperJob("score", scan, MyMapper.class, Text.class, IntWritable.class, job);
            //设置reducer
            TableMapReduceUtil.initTableReducerJob(tableName, MyReducer.class, job);
            //执行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("年级总分前10名排名统计完成");
                System.out.println("分数 学号 班级 姓名");
            }
            //构建Table对象
            Table studentTable = connection.getTable(tn);
            //定义行键、列簇、列名
            byte[] f = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("id");
            byte[] c2 = Bytes.toBytes("classId");
            byte[] c3 = Bytes.toBytes("name");
            byte[] c4 = Bytes.toBytes("score");
            //构建Scan对象
            Scan scan2 = new Scan();
            //执行读取操作
            ResultScanner rows = studentTable.getScanner(scan2);
            //读取值
            String id = "", classId = "", name = "";
            int score = 0, i = 0;
            for (Result row : rows) {
                //读取指定列簇、列名的单元格的值
                if (row.containsColumn(f, c1)) {
                    id = Bytes.toString(row.getValue(f, c1));
                }
                if (row.containsColumn(f, c2)) {
                    classId = Bytes.toString(row.getValue(f, c2));
                }
                if (row.containsColumn(f, c3)) {
                    name = Bytes.toString(row.getValue(f, c3));
                }
                if (row.containsColumn(f, c4)) {
                    score = Bytes.toInt(row.getValue(f, c4));
                }
                if (i == 10) {
                    break;
                }
                System.out.printf("%s\t%s\t%s\t%s%n", score, id, classId, name);
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
