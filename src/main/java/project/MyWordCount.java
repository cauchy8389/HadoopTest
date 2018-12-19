package project;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWordCount {
    private static final String HDFS_URL = "hdfs://zhy.cauchy8389.com:9000";
    //private static final String RESOUCEMANAGER_URL = "master16.example.com:8032";

    // 第一步： 对Maper的实现
    public static class WordCountMapper extends
            Mapper<LongWritable, Text, Text, LongWritable> {
        private Text mapKey = new Text();
        private LongWritable mapValue = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            // super.map(key, value, context);
            // System.err.println("key:" + key.get() + " value:"
            // + value.toString());
            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line);
            String word = null;
            while (st.hasMoreTokens()) {
                word = st.nextToken();
                // 数据传送给reducer
                mapKey.set(word);
                context.write(mapKey, mapValue);
                // System.err.println("key:" + word + " value:1");
            }
        }
    }

    // 第二步：对Reducer的实现
    public static class WordcountReducer extends
            Reducer<Text, LongWritable, Text, LongWritable> {
        LongWritable reduceValue = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            // TODO Auto-generated method stub
            // super.reduce(arg0, arg1, arg2);
            for (LongWritable var : values) {
                sum = sum + var.get();
            }


            // 把计算的结果交给output
            reduceValue.set(sum);
            context.write(key, reduceValue);
        }

    }

    // 第三步：控制map+reduce的流程
    public int run(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        int ret = -1;

        // 1. 获取指定hdfs的数据
        Configuration conf = new Configuration();
        // 1.1 与指定的hdfs连接
        conf.set("fs.defaultFS", HDFS_URL);

        // 1.2与指定的resourcemanager连接
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.address", RESOUCEMANAGER_URL);
//		conf.set("mapred.remote.os", "Windows");
//		conf.set("mapreduce.app-submission.cross-platform", "true");
//		String local = "C:\\Users\\student\\workspace\\day09.01-hadoop-mapreduce\\jars\\wordcount1.jar";
//		conf.set("mapred.jar", local);

        // 2. 得到一个作业对象
        Job job = Job.getInstance(conf);
        //job.setJar(local);
        // 3. 设置作业名称等信息
        // 作业名
        // job.setJobName(MyWordCount.class.getName());
        job.setJobName("MyWordCount");
        // 作业类
        job.setJarByClass(MyWordCount.class);

        // 4. input --> map --> reduce -->output
        // 4.1 input阶段
        Path inputpath = new Path("/user/zhy/logs/20170206.log");
        FileInputFormat.addInputPath(job, inputpath);

        // 4.2 map阶段
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 4.xx shuffle ....
        job.setCombinerClass(WordcountReducer.class);

        // 4.3 reduce 阶段
        job.setReducerClass(WordcountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 4.4 output阶段
        Path outputpath = new Path("/user/zhy/cleanlogs/20170206");
        FileSystem fs = outputpath.getFileSystem(conf);
        // 每次执行之前，先删除输出目录
        fs.delete(outputpath, true);

        FileOutputFormat.setOutputPath(job, outputpath);

        // 5. 任务的提交
        ret = job.waitForCompletion(true) ? 0 : -1;

        return ret;
    }

    // 第四步：启动任务的作业
    public static void main(String[] args) throws ClassNotFoundException,
            IOException, InterruptedException {
        System.setProperty("hadoop.home.dir",
                "E:\\hadoop-2.6.0-cdh5.8.5");
        MyWordCount job01 = new MyWordCount();
        job01.run(args);
    }
}
