package bulk;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount1 {
    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //从HDFS文件中读取数据。并拆分单词
            String line = value.toString();
            if (StringUtils.isBlank(line)) {
                return;
            }
            //拆分单词
            StringTokenizer st = new StringTokenizer(line);
            while (st.hasMoreTokens()) {
                String word = st.nextToken();
                //输出
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
    private static class MyReducer extends TableReducer<Text, IntWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            //汇总
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            //定义行键、列簇、列名、值
            byte[] rk = Bytes.toBytes(key.toString());
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("total");
            byte[] v1 = Bytes.toBytes(sum);
            //构建Put
            Put put = new Put(rk);
            put.addColumn(fm, c1, v1);
            //输出
            context.write(NullWritable.get(), put);
        }
    }
    public static void main(String[] args) {
        try {
            //构建配置对象：自动读取hbase-site.xml
            Configuration conf = HBaseConfiguration.create();
            //构建Connection对象
            Connection connection = ConnectionFactory.createConnection(conf);
            //构建Admin对象，用于DDL操作
            Admin admin = connection.getAdmin();
            //定义表名
            String tableName = "wordCount1";
            //构建TableName对象
            TableName tn = TableName.valueOf(tableName);
            //判断表是否存在，存在则删除之
            if (admin.tableExists(tn)) {
                //先禁用表
                admin.disableTable(tn);
                //再删除表
                admin.deleteTable(tn);
                //提示信息
                System.out.printf("表 %s 已存在，删除成功%n", tableName);
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
            System.out.printf("表 %s 创建成功%n", tableName);
            //设置输出表
            conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
            //创建Job任务
            Job job = Job.getInstance(conf, "word count");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/books"));
            //设置mapper
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            //设置reducer
            job.setReducerClass(MyReducer.class);
            //设置输出
            job.setOutputFormatClass(TableOutputFormat.class);
            //执行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("词频统计结束：从HDFS中读取数据，将词频统计的结果写入hbase的表wordCount1");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
