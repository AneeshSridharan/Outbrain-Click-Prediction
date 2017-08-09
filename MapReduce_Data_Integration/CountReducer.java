package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, Text, Text, Text> {

	@SuppressWarnings("unused")
	public void reduce(Text key, Iterable<Text> values, Context context)

	throws IOException, InterruptedException {

		int count = 0;
		for (Text value : values) {
			count++;
		}

		context.write(key, new Text(String.valueOf(count)));

	}
}
