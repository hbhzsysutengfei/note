package com.yang.hadoop.mapreduce.app.topk;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKMapReduceV4 {

	static class TopKMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		private Text mapOutputKey = new Text();
		private LongWritable mapOutputValue = new LongWritable();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			String[] strs = lineValue.split("\t");
			long tempValue = Long.valueOf(strs[1]);
			mapOutputKey.set(strs[0]);
			mapOutputValue.set(tempValue);

			context.write(mapOutputKey, mapOutputValue);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			super.cleanup(context);
		}
	}

	static class TopKReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		private static final int MAX_KEY = 3;

		TreeSet<TopKWritable> topSet = new TreeSet<TopKWritable>(
				new Comparator<TopKWritable>() {

					public int compare(TopKWritable o1, TopKWritable o2) {
						return o1.getCount().compareTo(o2.getCount());
					}
				});

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long count = 0L;

			for (LongWritable value : values) {
				count += value.get();
			}

			topSet.add(new TopKWritable(key.toString(), count));
			if (topSet.size() > MAX_KEY) {
				topSet.remove(topSet.first());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			for (TopKWritable top : topSet) {
				context.write(new Text(top.getWord()),
						new LongWritable(top.getCount()));
			}
		}
	}

	// dirver code

	public int run(String[] args) throws Exception {
		// get conf
		Configuration conf = new Configuration();

		// create job
		Job job = new Job(conf, TopKMapReduceV4.class.getSimpleName());

		// set job
		job.setJarByClass(TopKMapReduceV4.class);
		// job input
		Path inputDirPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputDirPath);
		
		// job map class
		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// job reduce class
		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// set reduce task number is 0, no reduce task
		job.setNumReduceTasks(1);

		// job output
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);

		// submit job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		args = new String[] { "hdfs://hadoop-master:9000/opt/data/wc/output7",
				"hdfs://hadoop-master:9000/opt/data/wc/output10" };

		int status = new TopKMapReduceV4().run(args);
		System.exit(status);
	}

}
