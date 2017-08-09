package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClickTrainMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (key.get() == 0){
			return;
		}				
		String line = value.toString();
		String[] input = line.split(",");
		String adClicked = "ad_id-"+input[1]+"-"+input[2];
		context.write(new Text(input[0]),new Text(adClicked));
	}
}
