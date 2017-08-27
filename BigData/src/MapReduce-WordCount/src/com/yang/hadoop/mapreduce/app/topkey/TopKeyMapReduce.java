package com.yang.hadoop.mapreduce.app.topkey;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKeyMapReduce {

	private static final int KEY = 10;

	// 1) map class
	public static class TopKeyMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();

			// valid
			if (null == lineValue) {
				return;
			}

			// split
			String[] strs = lineValue.split("\t");
			if (null != strs && strs.length == 5) {
				String languageType = strs[0];
				String singName = strs[1];
				String playTimes = strs[3];

				context.write(new Text(languageType + "\t" + singName),
						new LongWritable(Long.valueOf(playTimes)));
			}

		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

	}

	// 2) reduce class
	public static class TopKeyReduce extends
			Reducer<Text, LongWritable, TopKeyWritable, NullWritable> {

		TreeSet<TopKeyWritable> topSet = new TreeSet<TopKeyWritable>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			if (null == key) {
				return;
			}

			String[] splited = key.toString().split("\t");
			if (splited == null || splited.length == 0) {
				return;
			}

			String languageType = splited[0];
			String singName = splited[1];
			Long playTimes = 0L;
			for (LongWritable value : values) {
				playTimes += value.get();
			}
			topSet.add(new TopKeyWritable(languageType, singName, playTimes));

			if (topSet.size() > KEY) {
				topSet.remove(topSet.last());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (TopKeyWritable top : topSet) {
				context.write(top, NullWritable.get());
				;
			}
		}
	}

	// 3) Driver code
	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		// conf
		Configuration configuration = new Configuration();

		// create job
		Job job = new Job(configuration, TopKeyMapReduce.class.getSimpleName());

		// set job
		job.setJarByClass(TopKeyMapReduce.class);

		// input
		Path inputPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputPath);

		// map
		job.setMapperClass(TopKeyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// reduce
		job.setReducerClass(TopKeyReduce.class);
		job.setNumReduceTasks(1);

		// output
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);

		// start
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;

	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		args = new String[] { "hdfs://hadoop-master:9000/opt/data/topk/input",
				"hdfs://hadoop-master:9000/opt/data/topk/output" };
		int status = new TopKeyMapReduce().run(args);
		System.exit(status);
	}

	//

}
