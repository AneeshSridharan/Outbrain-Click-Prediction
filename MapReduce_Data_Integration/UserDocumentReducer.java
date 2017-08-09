package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserDocumentReducer extends Reducer<Text, Text, Text, Text> {

	@SuppressWarnings("unused")
	public void reduce(Text key, Iterable<Text> values, Context context)

	throws IOException, InterruptedException {

		int count = 0;
		for (Text value : values) {
			count++;
		}
		//Split the key. Save the Uuid as the key and docid as a separate value.
		String str = key.toString();
		String[] in = str.split("-");
		context.write(new Text(in[0]), new Text(in[1]+","+String.valueOf(count)));

	}
}
