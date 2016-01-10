package com.rajesh.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class SimplilearnAssignment
{

	private static Logger logger = Logger.getLogger(SimplilearnAssignment.class);

	// MAPPER

	// Input key=0 value=orange,apple,banana,mango,orange,apple,litchi
	// Output key = orange value=1
	public static class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		Text outkey = new Text();
		IntWritable outvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] words = value.toString().split(",");
			logger.info(words);
			if ("credit".equalsIgnoreCase(words[6]))
			{
				outkey.set(words[5]);
				outvalue.set(Integer.parseInt(words[3]));

				logger.info("Adding {" + words[5] + " , " + words[3] + "} to the context from Mapper");

				context.write(outkey, outvalue);
			}

		}
	}

	// REDUCER
	// Input key = orange value=1,1,1,1,1
	// Output key = orange, value = 5
	public static class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		IntWritable outvalue = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{

			logger.info("Reducer key " + key + "       values " + values);
			int sum = 0;
			for (IntWritable value : values)
			{
				sum = sum + value.get();
			}
			outvalue.set(sum);

			logger.info("Reducer key " + key + "       sum  " + sum);
			context.write(key, outvalue);
		}
	}

	// DRIVER
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration config = new Configuration();

		Job job = new Job(config, "Get Total Credit Card Amount in all places");

		job.setJarByClass(SimplilearnAssignment.class);
		job.setMapperClass(WcMapper.class);
		job.setReducerClass(WcReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPaths(job, args[0]); // "/user/cloudera/rajesh/MR1/"
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // "/user/cloudera/rajesh/MR1/output"

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
