package com.rajesh.hadoop.custom.datatype;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustType
{

	// MAPPER
	public static class CustMapper extends Mapper<LongWritable, Text, Text, Txn>
	{
		Text outkey = new Text();
		Txn outvalue = new Txn();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] columns = value.toString().split(",");
			outkey.set(columns[2]);
			outvalue.setAmount(Integer.parseInt(columns[3]));
			outvalue.setProduct(columns[4]);
			outvalue.setTxnDate(columns[1]);
			context.write(outkey, outvalue);
		}
	}

	// REDUCER
	public static class CustReducer extends Reducer<Text, Txn, Text, Text>
	{
		Text outvalue = new Text();

		public void reduce(Text key, Iterable<Txn> values, Context context) throws IOException, InterruptedException
		{
			Txn max = new Txn();
			for (Txn txn : values)
			{
				if (max.getAmount() < txn.getAmount())
				{
					max = txn;
				}
			}
			outvalue.set("Max amount = " + max.getAmount() + " & product = " + max.getProduct() + " & date = " + max.getTxnDate());
			context.write(key, outvalue);
		}
	}

	// DRIVER
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Custom Type Demo");

		job.setJarByClass(CustType.class);
		job.setMapperClass(CustMapper.class);
		job.setReducerClass(CustReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Txn.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
