package com.ucap.hadoop.mr.kpi;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ucap.hadoop.utils.HdfsClient;

public class KPIBrowser {

	public static class KPIBrowserMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			KPI kpi = KPI.filterBroswer(value.toString());
			if (kpi.isValid()) {
				word.set(kpi.getHttp_user_agent());
				context.write(word, one);
			}
		}
	}

	public static class KPIBrowserReducer
			extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String input = conf.get("fs.defaultFS") + "/user/dblues/log_kpi/";
		String output = conf.get("fs.defaultFS") + "/user/dblues/log_kpi_output/browser/";
		HdfsClient.getInstance().delete("/user/dblues/log_kpi_output/browser/");
		Job job = Job.getInstance(conf, "KPIBrowser");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(KPIBrowserMapper.class);
		job.setCombinerClass(KPIBrowserReducer.class);
		job.setReducerClass(KPIBrowserReducer.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
