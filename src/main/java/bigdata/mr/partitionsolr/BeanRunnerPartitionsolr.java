package bigdata.mr.partitionsolr;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BeanRunnerPartitionsolr {

	static class BeanMapper extends Mapper<LongWritable, Text, Bean, Text> {

		public Bean bean = new Bean();

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			String[] fields = StringUtils.split(line, "\t");

			String timestamp = fields[0];

			String ip = fields[1];

			String domain = fields[4];

			String width = fields[5];

			Text tx = new Text(timestamp + " \t " + ip);

			bean.set(domain, Long.parseLong(width));

			context.write(bean, tx);

		}

	}

	static class BeanReduce extends Reducer<Bean, Text, Bean, Text> {

		protected void reduce(Bean key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text bean : values) {
				context.write(key, bean);
			}

		}

	}

	public static void main(String[] args) throws Exception, IOException,
			InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(BeanRunnerPartitionsolr.class);

		job.setMapperClass(BeanMapper.class);
		job.setReducerClass(BeanReduce.class);

		// 指定自定义的partitioner类，替换掉框架默认的HashPartitioner
		job.setPartitionerClass(ProvincePartitioner.class);

		// 指定reduce task数量，跟ProvincePartitioner的分区数匹配
		job.setNumReduceTasks(6);

		job.setOutputKeyClass(Bean.class);
		job.setOutputValueClass(Text.class);

		String inputPath = "/usr/zhy/mapreduce/data";
		String outputPath = "/usr/zhy/mapreduce/solrandpartition";

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

		job.waitForCompletion(true);
	}

}
