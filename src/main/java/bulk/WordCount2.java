package bulk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

public class WordCount2 {
    public static class MyMapper extends TableMapper<Text, IntWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("line");
            if (value.containsColumn(fm, c1)) {
                //读取指定列的值
                String line = Bytes.toString(value.getValue(fm, c1));
                //拆分单词
                String[] items = line.split(" ");
                //输出
                for (String item : items) {
                    context.write(new Text(item), new IntWritable(1));
                }
            }
        }
    }
    public static class MyReducer extends TableReducer<Text, IntWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            //汇总
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            //定义行键、列键、列名、值
            byte[] rk = Bytes.toBytes(key.toString());
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("total");
            byte[] c2 = Bytes.toBytes(sum);
            //构建Put
            Put put = new Put(rk);
            put.addColumn(fm, c1, c2);
            //输出
            context.write(NullWritable.get(), put);
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
                String tableName = "wordCount2";
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
                Job job = Job.getInstance(conf, "word count");
                //Scan 读取源表中的数据
                Scan scan = new Scan();
                byte[] f = Bytes.toBytes("info");
                byte[] c = Bytes.toBytes("line");
                scan.addColumn(f, c);
                //设置mapper
                TableMapReduceUtil.initTableMapperJob("myBooks", scan, MyMapper.class, Text.class, IntWritable.class, job);
                //设置reducer
                TableMapReduceUtil.initTableReducerJob(tableName, MyReducer.class, job);
                //执行
                boolean flag = job.waitForCompletion(true);
                if (flag) {
                    System.out.println("词频统计结束：从hbase表myBooks中读取数据，将词频统计的结果写入hbase中的表wordCount2");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
