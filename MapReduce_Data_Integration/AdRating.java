package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AdRating {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		if (args.length != 2) {

			System.err
					.println("Usage: AdRating <hdfs_path-1> <output-path>");

			System.exit(-1);

		}
		String input1 = args[0];
		String output = args[1];
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");

		@SuppressWarnings("deprecation")
		Job job = new Job(conf);

		job.setJarByClass(AdRating.class);

		job.setJobName("Build implicit ad rating");

		FileInputFormat.addInputPath(job, new Path(input1));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(AdCountMapper.class);
		
		job.setReducerClass(AdRatingReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

	}
}
