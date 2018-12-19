package bigdata.mr.basicmr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/***
 *
 * <p>
 * Description: 基本的WordCount
 * </p>
 *
 * @author zhy
 * @date 2018年11月21日下午1:20:15
 * @version 1.0
 */

public class WordCountRunner {

	static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		int count = 0;

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			String[] fields = StringUtils.split(line, "\t");

			Text domain = new Text(fields[4]);

			context.write(new Text(domain), new LongWritable(1));

		}
	}

	static class WordCountReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		protected void reduce(Text key, Iterable<LongWritable> values,
							  Context context) throws IOException, InterruptedException {

			long counter = 0;

			for (LongWritable value : values) {

				// 累加每一个value
				counter += value.get();

			}

			context.write(key, new LongWritable(counter));
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		// 封装任务信息的对象为Job对象,所以要先构造一个Job对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf);

		// 设置本次job作业所在的jar包
		job.setJarByClass(WordCountRunner.class);

		// 本次job作业使用的mapper类是哪个？
		job.setMapperClass(WordCountMapper.class);
		// 本次job作业使用的reducer类是哪个？
		job.setReducerClass(WordCountReducer.class);

		// 本次job作业mapper类的输出数据key类型
		job.setMapOutputKeyClass(Text.class);
		// 本次job作业mapper类的输出数据value类型
		job.setMapOutputValueClass(LongWritable.class);

		// 本次job作业reducer类的输出数据key类型
		job.setOutputKeyClass(Text.class);
		// 本次job作业reducer类的输出数据value类型
		job.setOutputValueClass(LongWritable.class);

		job.setNumReduceTasks(1); // 设置reduce的个数

		String inputPath = "/usr/zhy/mapreduce/data";
		String outputPath = "/usr/zhy/mapreduce/wordcount";

		// 判断output文件夹是否存在，如果存在则删除
		Path path = new Path(outputPath);// 取第1个表示输出目录参数（第0个参数是输入目录）
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
		}

		// //本次job作业要处理的原始数据所在的路径
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		// //本次job作业产生的结果输出路径
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// 提交本次作业
		job.waitForCompletion(true);

	}

}
