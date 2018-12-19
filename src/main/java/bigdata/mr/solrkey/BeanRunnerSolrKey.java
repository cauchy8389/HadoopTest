package bigdata.mr.solrkey;

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

public class BeanRunnerSolrKey {

	static class BeanMapper extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			String[] fields = StringUtils.split(line, "\t");

			String timestamp = fields[0];

			String ip = fields[1];

			String domain = fields[4];

			Text width = new Text(fields[5]);

			Text tx = new Text(timestamp + " \t " + domain + " \t " + ip);

			context.write(width, tx);

		}

	}

	static class BeanReduce extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
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

		job.setJarByClass(BeanRunnerSolrKey.class);

		job.setMapperClass(BeanMapper.class);
		job.setReducerClass(BeanReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		String inputPath = "/usr/zhy/mapreduce/data";
		String outputPath = "/usr/zhy/mapreduce/sorlkey";

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
