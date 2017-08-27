package com.yang.hadoop.mr.module;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ModuleMapReduce extends Configured implements Tool {

	/*
	 * Mapper class
	 */
	public static class ModuleMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

	}

	/*
	 * Reducer class
	 */
	public static class ModuleReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(key, values, context);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

	}

	/*
	 * Drive code
	 */
	public Job parseInputAndOutput(Tool tool, Configuration conf, String[] args)
			throws IOException {
		// validate
		if (args.length != 2) {
			System.err.printf(
					"Useage: %s [generic options] <input> <output>\n", tool
							.getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return null;
		}

		// Step 2 create job
		Job job = new Job(conf, tool.getClass().getSimpleName());

		// 3. set input path
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// 14. set job output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job;
	}

	public int run(String[] args) throws Exception {

		// Step 1 get conf
		Configuration conf = new Configuration();

		Job job = parseInputAndOutput(this, conf, args);
		if (null == job) {
			return -1;
		}

		// 1. set run jar class
		job.setJarByClass(ModuleMapReduce.class);

		// 2. set inputfromat
		job.setInputFormatClass(TextInputFormat.class);

		// 4. set mapper
		job.setMapperClass(ModuleMapper.class);

		// 5. set map output key value class
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// 6. set partitioner class
		job.setPartitionerClass(HashPartitioner.class);

		// 7. set reduce number
		job.setNumReduceTasks(1);

		// 8. set sort comparator class
		// job.setSortComparatorClass(LongWritable.Comparator.class);

		// 9. set group comparator class
		// job.setGroupingComparatorClass(LongWritable.Comparator.class);

		// 10. set combiner class
		// job.setCombinerClass(null);

		// 11.set reduce class
		job.setReducerClass(ModuleReducer.class);

		// 12. set output Format
		job.setOutputFormatClass(TextOutputFormat.class);

		// 13. set job output key value class
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		// 14. set job output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// setp 4 submit job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		args = new String[] { 
				"hdfs://hadoop-master:9000/opt/data/topk/input",
				"hdfs://hadoop-master:9000/opt/data/topk/output" };
		int status = ToolRunner.run(new ModuleMapReduce(), args);
		System.exit(status);
	}

}
