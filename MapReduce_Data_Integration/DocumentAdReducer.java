package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocumentAdReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)

	throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder("");
		for (Text value : values) {
			if(value.getLength()>0) {
				sb.append(value).append(",");
			}
		}
		String ads = sb.toString();
		if(ads.length()>0) {
			ads = ads.substring(0, (ads.length()-1));
		}

		context.write(key, new Text(ads));

	}
}
