package com.skangyam.hadoop.mapreduce.VisitorCount;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VisitorCountReducer extends Reducer<Text, Text, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
	          throws IOException, InterruptedException{
		TreeSet<String> uniqUser = new TreeSet<String>();
		for (Text usr : values){
			uniqUser.add(usr.toString());
		}
		IntWritable noOfUniqUsers = new IntWritable(uniqUser.size());
		context.write(key, noOfUniqUsers);
	}

}
