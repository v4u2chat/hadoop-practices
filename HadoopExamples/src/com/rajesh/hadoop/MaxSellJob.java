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

public class MaxSellJob
{

	// MAPPER
	// Input key = 0, value = 0001,01-20-2013,400100,100,Exercise &
	// Fitness,clarksville,credit
	// Output key = Exercise & Fitness_clarksville, value = 100
	public static class MaxSellMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		Text outkey = new Text();
		IntWritable outvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] columns = value.toString().split(",");
			outkey.set(columns[4] + "_" + columns[5]);
			outvalue.set(Integer.parseInt(columns[3]));
			context.write(outkey, outvalue);
		}
	}

	// REDUCER
	// Input key = Exercise & Fitness_clarksville, value = (100,2000,450)
	// Output key = Exercise & Fitness_clarksville value= 2000
	public static class MaxSellReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		IntWritable outvalue = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int max = 0;
			for (IntWritable value : values)
			{
				if (max < value.get())
				{
					max = value.get();
				}
			}
			outvalue.set(max);
			context.write(key, outvalue);

		}
	}

	// DRIVER

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Word Count Job");

		job.setJarByClass(MaxSellJob.class);
		job.setMapperClass(MaxSellMapper.class);
		job.setReducerClass(MaxSellReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
