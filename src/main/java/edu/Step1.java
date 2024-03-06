package edu;

import bulk.WordCount1;
import bulk.WordCount2;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step1 {
    private static class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //id,name,classid,exam_date,chinese,math,english
            //10001,郑烟,1,2022-6-8,71,32,48
            //10002,郑烟,2,2022-6-8,21,45,63
            String line = value.toString();
            //空数据
            if (StringUtils.isEmpty(line)) {
                return;
            }
            //忽略标题
            if (line.startsWith("id")) {
                return;
            }
            //拆分数据
            String[] items = line.split(",");
            //忽略因块的拆分导致不在一个块的行
            if (items.length != 7) {
                return;
            }
            //输出:("10001", "郑烟,1,2022-6-8,71,32,48“）
            context.write(new Text(items[0]), new Text(line.substring(line.indexOf(",") + 1)));
        }
    }
    private static class Step1Reducer extends TableReducer<Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            //拆分
            String id = key.toString();
            String name = "", classId = "", examDate = "";
            int chinese = 0, math = 0, english = 0;
            for (Text v : values) {
                String[] items = v.toString().split(",");
                name = items[0];
                classId = items[1];
                examDate = items[2];
                chinese = Integer.parseInt(items[3]);
                math = Integer.parseInt(items[4]);
                english = Integer.parseInt(items[5]);
            }
            //构建put
            byte[] rk = Bytes.toBytes(id);
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            byte[] c2 = Bytes.toBytes("classId");
            byte[] c3 = Bytes.toBytes("examDate");
            byte[] c4 = Bytes.toBytes("chinese");
            byte[] c5 = Bytes.toBytes("math");
            byte[] c6 = Bytes.toBytes("english");
            byte[] v1 = Bytes.toBytes(name);
            byte[] v2 = Bytes.toBytes(classId);
            byte[] v3 = Bytes.toBytes(examDate);
            byte[] v4 = Bytes.toBytes(chinese);
            byte[] v5 = Bytes.toBytes(math);
            byte[] v6 = Bytes.toBytes(english);
            Put put = new Put(rk);
            put.addColumn(fm, c1, v1);
            put.addColumn(fm, c2, v2);
            put.addColumn(fm, c3, v3);
            put.addColumn(fm, c4, v4);
            put.addColumn(fm, c5, v5);
            put.addColumn(fm, c6, v6);
            //输出到Hbase表中
            context.write(NullWritable.get(), put);
        }
    }
    public static void main(String[] args) {
        try {
            //构建配置对象
            Configuration conf = HbaseUtils.getConf();
            //定义表名
            String tableName = "score";
            //创建表
            HbaseUtils.createTable(tableName, "info");
            //设置输出表
            conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
            //创建job任务
            Job job = Job.getInstance(conf, "score import");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/edu"));
            //设置mapper
            job.setMapperClass(Step1Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //设置reducer
            job.setReducerClass(Step1Reducer.class);
            //设置输出
            job.setOutputFormatClass(TableOutputFormat.class);
            //执行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("成绩信息数据导入成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


