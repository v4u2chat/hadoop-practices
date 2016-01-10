package com.rajesh.hadoop.custom.counter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomCounter
{

	public static enum RECORD_CHECK
	{
		GOOD, BAD
	}

	// input key = 0,value = Hi This is GOOD record
	// output key = GOOD value = count
	public static class CounterMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>
	{
		Text outkey = new Text();
		IntWritable outvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			if (value.toString().contains("GOOD"))
			{
				context.getCounter(RECORD_CHECK.GOOD).increment(1);
			} else if (value.toString().contains("BAD"))
			{
				context.getCounter(RECORD_CHECK.BAD).increment(1);
			}
			context.write(NullWritable.get(), NullWritable.get());
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Custom Counter Job");

		job.setJarByClass(CustomCounter.class);
		job.setMapperClass(CounterMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		Counters counters = job.getCounters();
		System.out.println("Total Number of GOOD records = " + counters.findCounter(RECORD_CHECK.GOOD));
		System.out.println("Total Number of BAD records = " + counters.findCounter(RECORD_CHECK.BAD));

	}

}
