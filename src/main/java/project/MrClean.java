package project;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang3.time.DateUtils;

//import bigdata.utils.DateUtils;

//cn.orcale.com.project.mrClean
public class MrClean {


	//map将输入中的value复制到输出数据的key上，并直接输出
	public static class Map extends Mapper<Object,Text,Text,Text>{

		//实现map函数
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{

			DateUtils du = new DateUtils();

			String day = "";

			String hour = "";

			String[] fields = value.toString().split("\\s");

			StringBuffer sb = new StringBuffer();

			// 过滤字段少的数据
			//if (fields.length < 15) {
			//	return;
			//}

			// 过滤domain为空
			if (StringUtils.isBlank(fields[4])) {
				return;
			}


			try {
				Date date = DateUtils.parseDate(fields[4].substring(1, 21), Locale.US,"dd/MMM/yyyy:HH:mm:ss");

				//Date dt1 = DateUtils.parseDate("2011-07-08", DateFormatUtils.ISO_DATE_FORMAT.getPattern());

				day = DateFormatUtils.ISO_DATE_FORMAT.format(date);
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				hour = String.valueOf(cal.get(Calendar.HOUR_OF_DAY));

			} catch (ParseException e) {
				e.printStackTrace();
			};

			//hour = du.stampToDate(fields[4].substring(0, 10)+"000").substring(11,13);

			//for(int i = 0; i<fields.length ; i++){
			sb.append(fields[0]+'\t');
			sb.append(fields[1]+'\t');
			sb.append(fields[fields.length - 1]+'\t');
			//}

			sb.append(day +'\t' + hour) ;

			context.write(new Text(sb.toString()),  new Text(""));
		}
	}


	//reduce将输入中的key复制到输出数据的key上，并直接输出

	public static class Reduce extends Reducer<Text,Text,Text,Text>{

		//实现reduce函数

		public void reduce(Text key,Iterable<Text> values,Context context)

				throws IOException,InterruptedException{

			context.write(key, new Text(""));

		}
	}



	public static void main(String[] args) throws Exception{

		System.setProperty("hadoop.home.dir",
				"E:\\hadoop-2.6.0-cdh5.8.5");


		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://zhy.cauchy8389.com:9000");

		Job job = Job.getInstance(conf);

		job.setJobName("MrClean");
		job.setJarByClass(MrClean.class);


		//设置Map、Combine和Reduce处理类
		job.setMapperClass(Map.class);

		job.setReducerClass(Reduce.class);


		//指定reduce task数量，跟ProvincePartitioner的分区数匹配
		job.setNumReduceTasks(1);

		//本次job作业mapper类的输出数据key类型
		job.setMapOutputKeyClass(Text.class);
		//本次job作业mapper类的输出数据value类型
		job.setMapOutputValueClass(Text.class);

		//本次job作业reducer类的输出数据key类型
		job.setOutputKeyClass(Text.class);
		//本次job作业reducer类的输出数据value类型
		job.setOutputValueClass(Text.class);

		String inputPath = "";
		if(args == null || args.length ==0){
			inputPath = "/user/zhy/logs/20170206.log";
		}else {
			inputPath = "/user/zhy/logs/" + args[0] + ".log";
		}

		String outputPath = "";
		if(args == null || args.length ==0){
			outputPath = "/user/zhy/cleanlogs/20170206";
		}else {
			outputPath = "/user/zhy/cleanlogs/" + args[0];
		}

		//String ddate=args[0].substring(0, 8);//2015010112
		//System.out.println("ddate:="+ddate);
		//String hour=args[0].substring(8,args[0].length());
		//System.out.println("hour:="+hour);
		//String outputPath = "/user/yuhui/cleanlogs/"+ddate+"/"+hour;

		// 判断output文件夹是否存在，如果存在则删除
		Path path = new Path(outputPath);// 取第1个表示输出目录参数（第0个参数是输入目录）
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
		}

//		//本次job作业要处理的原始数据所在的路径
		FileInputFormat.setInputPaths(job,  new Path(inputPath));
//		//本次job作业产生的结果输出路径
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

	}

}
