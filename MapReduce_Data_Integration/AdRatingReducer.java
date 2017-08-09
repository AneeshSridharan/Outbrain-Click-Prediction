package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AdRatingReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException {
		double total = 0;
		double clicked = 0;
		double rating = 0;
		for (Text value : values) {
			if(value.charAt(0)=='1') {
				clicked++;
			} 
			total++;
		}
		rating = clicked/total;
		context.write(key, new Text(String.valueOf(rating)));

	}
}
