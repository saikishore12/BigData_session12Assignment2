package com.acadgild;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public  class HospitalReducer extends
Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	    throws IOException, InterruptedException {

		int sum = 0;
		int count=0;
		int Answer_percentage;
		for (IntWritable value : values) {
			sum += value.get();
			count++;
		}
		Answer_percentage=sum/count;
		context.write(key, new IntWritable(Answer_percentage));
		
		
	}
}
