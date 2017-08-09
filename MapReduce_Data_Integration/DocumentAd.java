package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DocumentAd {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {

			System.err
					.println("Usage: DocumentAd <previous_reduced_result> <output path>");

			System.exit(-1);

		}
		String input1 = args[0];
		String output = args[1];
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);

		job.setJarByClass(DocumentAd.class);

		job.setJobName("Get all ads for each document");

		FileInputFormat.addInputPath(job, new Path(input1));
		FileInputFormat.setInputDirRecursive(job, true);

		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(DocumentAdMapper.class);

		job.setReducerClass(DocumentAdReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);		
		
		job.waitForCompletion(true);		
	}

}
