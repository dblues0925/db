package com.ucap.hadoop.mr.kpi;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.ucap.hadoop.mr.kpi.KPIBrowser.KPIBrowserMapper;
import com.ucap.hadoop.mr.kpi.KPIBrowser.KPIBrowserReducer;
import com.ucap.hadoop.utils.HdfsClient;

public class KPITime {

	public static class KPITimeMapper extends Mapper<Object, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			KPI kpi = KPI.filterBroswer(value.toString());
			if (kpi.isValid()) {
				try {
					word.set(kpi.getTime_local_Date_hour());
					context.write(word, one);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static class KPITimeReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			while (values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String input = conf.get("fs.defaultFS") + "/user/dblues/log_kpi/";
		String output = conf.get("fs.defaultFS") + "/user/dblues/log_kpi_output/time/";
		HdfsClient.getInstance().delete("/user/dblues/log_kpi_output/time/");
		Job job = Job.getInstance(conf, "KPITime");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(KPITimeMapper.class);
		job.setCombinerClass(KPITimeReducer.class);
		job.setReducerClass(KPITimeReducer.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
