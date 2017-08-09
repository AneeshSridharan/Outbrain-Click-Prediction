package edu.nyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocumentAdMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] input = line.split(",");
		int docIdLoc = 0;
		StringBuilder sd = new StringBuilder("");
		for (int i = 1; i < input.length; i++) {
			if(input[i].startsWith("ad")) {
				sd.append(input[i]).append(",");
			} else {
				//first field after the ads is the document id
				docIdLoc = i;
				break;
			}
		}
		String ads = sd.toString();
		if(ads.length()>0) {
			ads = ads.substring(0, (ads.length()-1));
		}
		context.write(new Text(input[docIdLoc]),new Text(ads));
	}
}
