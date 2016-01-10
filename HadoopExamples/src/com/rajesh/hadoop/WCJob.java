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

public class WCJob
{

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
			for (String word : words)
			{
				outkey.set(word);
				outvalue.set(1);
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
			int sum = 0;
			for (IntWritable value : values)
			{
				sum = sum + value.get();
			}
			outvalue.set(sum);
			context.write(key, outvalue);
		}
	}

	// DRIVER
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Word Count Job");

		job.setJarByClass(WCJob.class);
		job.setMapperClass(WcMapper.class);
		job.setReducerClass(WcReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
