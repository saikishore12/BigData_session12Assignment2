package com.acadgild;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class HospitalMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {
	private Text Hospital_Name = new Text();
	private IntWritable Answer_percent = new IntWritable();
@Override
public void map(LongWritable key, Text value, Context context)
  throws IOException, InterruptedException {

	String line = value.toString();
	String hospital_data[] = line.split(",");

	if (hospital_data.length > 4) {
		Hospital_Name.set(hospital_data[0]);
		Answer_percent.set(Integer.parseInt(hospital_data[4]));
	}

	context.write(Hospital_Name, Answer_percent);
  }
}
