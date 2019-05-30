package com.ucap.hadoop.mr.kpi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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

public class KPIIP {

    public static class KPIIPMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text ips = new Text();
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            KPI kpi = KPI.filterIPs(value.toString());
            if (kpi.isValid()) {
                ips.set(kpi.getRemote_addr());
                context.write(ips, one);
            }
        }
    }

    public static class KPIIPReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
    	private IntWritable result = new IntWritable();
        private Set<String> count = new HashSet<String>();

        @Override
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
		String output = conf.get("fs.defaultFS") + "/user/dblues/log_kpi_output/ip/";
		HdfsClient.getInstance().delete("/user/dblues/log_kpi_output/ip/");
		Job job = Job.getInstance(conf, "KPIIP");
		job.setMapperClass(KPIIPMapper.class);
		job.setCombinerClass(KPIIPReducer.class);
		job.setReducerClass(KPIIPReducer.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
