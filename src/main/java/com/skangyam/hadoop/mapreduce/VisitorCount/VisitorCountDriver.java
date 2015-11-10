package com.skangyam.hadoop.mapreduce.VisitorCount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VisitorCountDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] "
					+ "<input dir> <output Directory> \n",
					getClass().getSimpleName());
			return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(VisitorCountDriver.class);
		job.setJobName(this.getClass().getName());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(VisitorCountMapper.class);
		job.setReducerClass(VisitorCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		if (job.waitForCompletion(true)) {
			return 0;
		}
		
		return 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new VisitorCountDriver(), args);
		System.exit(exitCode);

	}

}
