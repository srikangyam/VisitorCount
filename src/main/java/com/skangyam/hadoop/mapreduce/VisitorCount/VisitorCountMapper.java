package com.skangyam.hadoop.mapreduce.VisitorCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class VisitorCountMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	@Override
    protected void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException
    {
		String[] str = value.toString().split("\\|");
        context.write(new Text(str[1]), new Text(str[0]));
    }
}
