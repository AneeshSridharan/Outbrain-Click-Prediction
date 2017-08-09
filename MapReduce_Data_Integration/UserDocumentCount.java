package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserDocumentCount {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {

			System.err
					.println("Usage: UserDocumentCount <pageview> <output path>");

			System.exit(-1);

		}
		String input1 = args[0];
		String output = args[1];
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);

		job.setJarByClass(UserDocumentCount.class);

		job.setJobName("Count User-document");

		FileInputFormat.addInputPath(job, new Path(input1));

		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(UserDocumentMapper.class);

		job.setReducerClass(UserDocumentReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);		
		
		job.waitForCompletion(true);		
	}

}
