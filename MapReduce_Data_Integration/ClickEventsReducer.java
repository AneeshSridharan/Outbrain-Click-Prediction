package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClickEventsReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)

	throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		for (Text value : values) {
			if(value.charAt(0)=='a') {
				sb.insert(0, (value+","));
			} else {
				sb.append(value);
			}
		}
		context.write(key, new Text(sb.toString()));

	}
}
