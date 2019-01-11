package bigdata.es.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;


public class Driver {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir",
                "E:\\hadoop-2.6.0-cdh5.8.5");

        Configuration conf = new Configuration();
        // ElasticSearch Server nodes to point to
        conf.set("es.nodes", "zhy.cauchy8389.com:9200");
        // ElasticSearch index and type name in {indexName}/{typeName} format
        conf.set("es.resource", "esh_complaints/complaints");
        conf.set("fs.defaultFS", "hdfs://zhy.cauchy8389.com:9000");

        // Create Job instance
        Job job = Job.getInstance(conf);
        job.setJobName("complaints mapper");
        // set Driver class
        job.setJarByClass(Driver.class);
        job.setMapperClass(ComplaintsMapper.class);
        // set OutputFormat to EsOutputFormat provided by ElasticSearch-Hadoop jar
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setNumReduceTasks(0);
        //job.setm
        FileInputFormat.addInputPath(job, new Path("/user/zhy/eshadoop/consumer_complaints.csv"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}