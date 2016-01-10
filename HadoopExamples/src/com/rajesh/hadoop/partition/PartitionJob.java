package com.rajesh.hadoop.partition;

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

public class PartitionJob
{

	// MAPPER
	// Input key = 0, value = 1 user1 permanent 100
	// output key = permanent value = 100
	public static class PartMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		Text outkey = new Text();
		IntWritable outvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] columns = value.toString().split(" ");
			outkey.set(columns[2]);
			outvalue.set(Integer.parseInt(columns[3]));
			context.write(outkey, outvalue);
		}
	}

	// REDUCER
	// Input key = permanent value = (100,300,150,100,130,200,100)
	// output key = permanent value = Avg of salary
	public static class PartReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		IntWritable outvalue = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0, avg = 0, count = 0;
			for (IntWritable value : values)
			{
				sum = sum + value.get();
				count++;
			}
			avg = sum / count;
			outvalue.set(avg);
			context.write(key, outvalue);
		}
	}

	// DRIVER

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "My Partition Demo Job");

		job.setJarByClass(PartitionJob.class);
		job.setMapperClass(PartMapper.class);
		job.setReducerClass(PartReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(3);
		job.setPartitionerClass(CustomPartitioner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
