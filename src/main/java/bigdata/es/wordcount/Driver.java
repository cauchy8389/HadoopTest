package bigdata.es.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;


public class Driver {

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir",
				"F:\\hadoop-2.6.0-cdh5.8.5");

		Configuration conf = new Configuration();
        // ElasticSearch Server nodes to point to
		conf.set("es.nodes", "zhy.cauchy8389.com:9200");
        // ElasticSearch index and type name in {indexName}/{typeName} format
		conf.set("es.resource", "eshadoop/wordcount");
		conf.set("fs.defaultFS", "hdfs://zhy.cauchy8389.com:9000");

        // Create Job instance
		Job job = Job.getInstance(conf);
		job.setJobName("word_count");
        // set Driver class
		job.setJarByClass(Driver.class);
        job.setMapperClass(WordsMapper.class);
        job.setReducerClass(WordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // set OutputFormat to EsOutputFormat provided by ElasticSearch-Hadoop jar
	    job.setOutputFormatClass(EsOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/user/zhy/es/sample.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/zhy/es/result"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
