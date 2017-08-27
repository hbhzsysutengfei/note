package com.yang.hadoop.mr.min;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MinimalMapReduce {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// conf
		Configuration conf = new Configuration();

		// create job
		Job job = new Job(conf, MinimalMapReduce.class.getSimpleName());

		// set job
		job.setJarByClass(MinimalMapReduce.class);

		job.setMapperClass(Mapper.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(Reducer.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		// 默认情况下， reduce输入的key/value类型与输出的key/value类型相同

		job.setCombinerClass(null);
		job.setPartitionerClass(HashPartitioner.class);

		job.setSortComparatorClass(LongWritable.Comparator.class);
		job.setGroupingComparatorClass(LongWritable.Comparator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// set input output
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean isSuccess = job.waitForCompletion(true);
		int status = isSuccess ? 0 : 1;
		System.exit(status);
	}
}
