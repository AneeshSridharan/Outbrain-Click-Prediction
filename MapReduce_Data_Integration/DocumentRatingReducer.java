package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocumentRatingReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)

	throws IOException, InterruptedException {
		double total = 0;
		double count = 0;
		String docId = "";
		StringBuilder sb = new StringBuilder();
		for (Text value : values) {
			String str = value.toString();
			if(str.startsWith("total")) {
				String[] input = str.split("=");
				total = Double.valueOf(input[1]);				
			} else {
				String[] input = str.split(",");
				count = Double.valueOf(input[1]);
				docId = input[0];
			}
		}
		double rating = count/total;
		sb.append(docId).append(",").append(rating);
		context.write(key, new Text(sb.toString()));

	}
}
