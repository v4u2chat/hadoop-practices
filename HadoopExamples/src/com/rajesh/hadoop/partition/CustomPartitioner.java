package com.rajesh.hadoop.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, IntWritable>
{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions)
	{
		int index = 0;

		if (key.toString().equals("permanent"))
		{
			index = 1;
		} else
		{
			index = 0;
		}

		return index;
	}

}
