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

public class Step3 {
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
            if (value.containsColumn(fm, c2)) {
                //读取指定列的值
                String line1 = Bytes.toString(value.getValue(fm, c2));
                int line2 = Bytes.toInt(value.getValue(fm, c4));
                int line3 = Bytes.toInt(value.getValue(fm, c5));
                int line4 = Bytes.toInt(value.getValue(fm, c6));
                int total = line2 + line3 + line4;
                //输出
                context.write(new Text(line1), new IntWritable(total));
            }
        }
    }
    public static class MyReducer extends TableReducer<Text, IntWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            int sum = 0, count = 0;
            for (IntWritable v : values) {
                sum += v.get();
                count++;
            }
            //定义行键、列键、列名、值
            byte[] rk = Bytes.toBytes(key.toString());
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("total");
            byte[] c2 = Bytes.toBytes("average");
            byte[] v1 = Bytes.toBytes(sum);
            byte[] v2 = Bytes.toBytes(sum/count);
            //构建Put
            Put put = new Put(rk);
            put.addColumn(fm, c1, v1);
            put.addColumn(fm, c2, v2);
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
            String tableName = "scoreOFClass";
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
            Job job = Job.getInstance(conf, "class count");
            //Scan 读取源表中的数据
            Scan scan = new Scan();
            //设置mapper
            TableMapReduceUtil.initTableMapperJob("score", scan, MyMapper.class, Text.class, IntWritable.class, job);
            //设置reducer
            TableMapReduceUtil.initTableReducerJob(tableName, MyReducer.class, job);
            //执行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("班级总分与平均分统计完成");
                System.out.println("班级编号 总分 平均分");
            }
            //构建Table对象
            Table studentTable = connection.getTable(tn);
            //定义行键、列簇、列名
            byte[] f = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("total");
            byte[] c2 = Bytes.toBytes("average");
            //构建Scan对象
            Scan scan2 = new Scan();
            //执行读取操作
            ResultScanner rows = studentTable.getScanner(scan2);
            //读取值
            String classId ="";
            int total = 0, average = 0;
            for (Result row : rows) {
                classId = Bytes.toString(row.getRow());
                //读取指定列簇、列名的单元格的值
                if (row.containsColumn(f, c1)) {
                    total = Bytes.toInt(row.getValue(f, c1));
                }
                if (row.containsColumn(f, c2)) {
                    average = Bytes.toInt(row.getValue(f, c2));
                }
                //输出整行数据
                System.out.printf("%s\t%d\t%d%n", classId, total, average);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
