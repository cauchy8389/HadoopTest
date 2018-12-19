package bigdata.mr.partition01;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class BeanRunner {

	static class BeanMapper extends Mapper<LongWritable, Text, Text, Bean> {

		public Bean tx = new Bean();

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			String[] fields = StringUtils.split(line, "\t");

			String timestamp = fields[0];

			String ip = fields[1];

			Text domain = new Text(fields[4]);

			// 将上下行流量封装到flowBean中去
			tx.set(timestamp, ip);

			context.write(domain, tx);

		}

	}

	static class BeanReduce extends Reducer<Text, Bean, Text, Bean> {

		protected void reduce(Text key, Iterable<Bean> values, Context context)
				throws IOException, InterruptedException {

			for (Bean bean : values) {
				context.write(key, bean);
			}

		}

	}

	public static void main(String[] args) throws Exception, IOException,
			InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(BeanRunner.class);

		job.setMapperClass(BeanMapper.class);
		job.setReducerClass(BeanReduce.class);

		// 指定自定义的partitioner类，替换掉框架默认的HashPartitioner
		job.setPartitionerClass(ProvincePartitioner.class);

		// 指定reduce task数量，跟ProvincePartitioner的分区数匹配
		job.setNumReduceTasks(6);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Bean.class);

		String inputPath = "/usr/zhy/mapreduce/data";
		String outputPath = "/usr/zhy/mapreduce/Partitioner";

		// 判断output文件夹是否存在，如果存在则删除
		Path path = new Path(outputPath);// 取第1个表示输出目录参数（第0个参数是输入目录）
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
		}

		// 要处理的数据所在的path
		// 指定文件夹即可，该文件夹下的所有文件都会被处理
		FileInputFormat.setInputPaths(job, new Path(inputPath));

		// 处理完得到的结果输出的path
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

}
