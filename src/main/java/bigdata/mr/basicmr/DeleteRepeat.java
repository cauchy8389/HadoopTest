package bigdata.mr.basicmr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//cn.orcale.com.bigdata.mr.basicmr.DeleteRepeat

/***
 *
 * @author zhy
 *
 */
public class DeleteRepeat {

	// map将输入中的value复制到输出数据的key上，并直接输出

	public static class Map extends Mapper<Object, Text, Text, Text> {

		private static Text line = new Text();// 每行数据

		// 实现map函数

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			System.out.println("line====>" + line);

			String[] fields = StringUtils.split(line, "\t");

			Text domain = new Text(fields[4]);

			context.write(new Text(domain), new Text(""));
		}
	}

	// reduce将输入中的key复制到输出数据的key上，并直接输出

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		// 实现reduce函数

		public void reduce(Text key, Iterable<Text> values, Context context)

				throws IOException, InterruptedException {

			context.write(key, new Text(""));

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(DeleteRepeat.class);

		// 设置Map、Combine和Reduce处理类
		job.setMapperClass(Map.class);

		job.setReducerClass(Reduce.class);

		// 设置输出类型

		// 本次job作业mapper类的输出数据key类型
		job.setMapOutputKeyClass(Text.class);
		// 本次job作业mapper类的输出数据value类型
		job.setMapOutputValueClass(Text.class);

		// 本次job作业reducer类的输出数据key类型
		job.setOutputKeyClass(Text.class);
		// 本次job作业reducer类的输出数据value类型
		job.setOutputValueClass(Text.class);

		String inputPath = "/usr/xiaohui/mapreduce/data";
		String outputPath = "/usr/xiaohui/mapreduce/deleteRepeat";

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