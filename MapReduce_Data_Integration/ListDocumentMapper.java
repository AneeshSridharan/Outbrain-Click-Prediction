package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ListDocumentMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (key.get() == 0){
			return;
		}				
		String line = value.toString();
		String[] input = line.split(",");
		context.write(new Text(input[2]),new Text(""));
	}
}
