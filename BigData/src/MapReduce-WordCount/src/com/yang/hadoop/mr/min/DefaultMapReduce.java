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

public class DefaultMapReduce {
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{
		args = new String[] { 
				"hdfs://hadoop-master:9000/opt/data/topk/input",
				"hdfs://hadoop-master:9000/opt/data/topk/output" };
		//Step 1 get  conf
		Configuration conf = new Configuration();
		
		//Step 2 create job
		Job job = new Job(conf, DefaultMapReduce.class.getSimpleName());
		
		//Step 3 set job
		
		//1. set run jar class
		job.setJarByClass(DefaultMapReduce.class);
		
		//2. set inputfromat 
		job.setInputFormatClass(TextInputFormat.class);
		
		//3. set input path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//4. set mapper
		job.setMapperClass(Mapper.class);
		
		//5. set map output key value class
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//6. set partitioner class
		job.setPartitionerClass(HashPartitioner.class);
		
		//7. set reduce number
		job.setNumReduceTasks(1);
		
		//8. set sort comparator class
		job.setSortComparatorClass(LongWritable.Comparator.class);
		
		//9. set group comparator class
		job.setGroupingComparatorClass(LongWritable.Comparator.class);
		
		//10. set combiner class
//		job.setCombinerClass(null);
		
		//11.set reduce class
		job.setReducerClass(Reducer.class);
		
		//12. set output Format
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//13. set job output key value class
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		//14. set job output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//setp 4 submit job
		boolean isSuccess = job.waitForCompletion(true);
		
		//step 5 exit program 
		System.exit(isSuccess?0:1);
	}
}
