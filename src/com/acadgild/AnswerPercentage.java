//Collect frequency of all the words who have a length more than some specified dynamic value

package com.acadgild;



import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class AnswerPercentage extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
      int exitCode = ToolRunner.run(new AnswerPercentage(), args);
      System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
      // Check arguments.
    	if (args.length != 2) {
        String usage =
          "Usage: " +
          "hadoop jar Configurations " +
          "<input dir> <output dir>\n";
        System.out.printf(usage);
        System.exit(-1);
      }
 
      String jobName = "Hospital-percentage";

      String inputDir = args[0];
      String outputDir = args[1];

      // Define input path and output path.
      Path inputPath = new Path(inputDir);
      Path outputPath = new Path(outputDir);
      Configuration configuration = getConf();
      Job job = Job.getInstance(configuration);
      job.setJobName(jobName);
      job.setJarByClass(AnswerPercentage.class);
      job.setMapperClass(HospitalMapper.class);
      job.setReducerClass(HospitalReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      TextInputFormat.setInputPaths(job, inputPath);

      job.setOutputFormatClass(TextOutputFormat.class);
      FileOutputFormat.setOutputPath(job, outputPath);

      // Submit the map-only job.
      return job.waitForCompletion(true) ? 0 : 1;
    }
  
  
  
  
}