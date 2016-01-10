package com.rajesh.hadoop.custom.inputformat;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyXmlMr
{

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException
		{
			String document = value.toString();
			System.out.println("‘" + document + "‘");
			try
			{
				XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(new ByteArrayInputStream(document.getBytes()));
				String propertyName = "";
				String propertyValue = "";
				String currentElement = "";
				while (reader.hasNext())
				{
					int code = reader.next();
					switch (code)
					{
					case XMLStreamConstants.START_ELEMENT: // START_ELEMENT:
						currentElement = reader.getLocalName();
						break;
					case XMLStreamConstants.CHARACTERS: // CHARACTERS:
						if (currentElement.equalsIgnoreCase("name"))
						{
							propertyName += reader.getText();
							// System.out.println(propertyName);
						} else if (currentElement.equalsIgnoreCase("value"))
						{
							propertyValue += reader.getText();
							// System.out.println(propertyValue);
						}
						break;
					}
				}
				reader.close();
				context.write(new Text(propertyName.trim()), new Text(propertyValue.trim()));

			} catch (Exception e)
			{
				throw new IOException(e);

			}

		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<property>");
		conf.set("xmlinput.end", "</property>");
		Job job = new Job(conf);
		job.setJarByClass(MyXmlMr.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMapper.class);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
