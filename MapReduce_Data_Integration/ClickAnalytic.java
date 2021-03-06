package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClickAnalytic {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		if (args.length != 3) {

			System.err
					.println("Usage: ClickAnalytic <hdfs_path-1> <hdfs-path-2> <output-path>");

			System.exit(-1);

		}
		String input1 = args[0];
		String input2 = args[1];
		String output = args[2];
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");

		@SuppressWarnings("deprecation")
		Job job = new Job(conf);

		job.setJarByClass(ClickAnalytic.class);

		job.setJobName("Multiple mappers");

		//FileInputFormat.addInputPath(job, new Path(input1));
		MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, ClickTrainMapper.class);
		MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class, EventsMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(output));

		
		job.setReducerClass(ClickEventsReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

	}

}
