package com.yang.hadoop.mapreduce.app.topk;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKMapReduceV2 {

	static class TopKMapper extends
			Mapper<LongWritable, Text, LongWritable, NullWritable> {

		private static final int MAX_KEY = 3;

		// long topkValue = Long.MIN_VALUE;
		//TreeMap<LongWritable, Text> topMap = new TreeMap<LongWritable, Text>();
		TreeSet<Long> topKey = new TreeSet<Long>();
		
		// map output key & map output value
		private LongWritable mapKey = new LongWritable();
		private Text mapValue = new Text();

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

//			mapKey.set(tempValue);
//			mapValue.set(strs[0]);

			topKey.add(tempValue);
			if (topKey.size() > MAX_KEY) {
				topKey.remove(topKey.first());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			LongWritable setKey = new LongWritable();
			for(Long key:topKey){
				setKey.set(key);
				context.write(setKey,NullWritable.get());
			}
		}
	}

	// dirver code

	public int run(String[] args) throws Exception {
		// get conf
		Configuration conf = new Configuration();

		// create job
		Job job = new Job(conf, TopKMapReduceV2.class.getSimpleName());

		// set job
		job.setJarByClass(TopKMapReduceV2.class);
		// job input
		Path inputDirPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputDirPath);
		// job map class
		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// job reduce class
		// job.setReducerClass(DataTotalReducer.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(LongWritable.class);

		// set reduce task number is 0, no reduce task
		job.setNumReduceTasks(0);

		// job output
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);

		// submit job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		args = new String[] { "hdfs://hadoop-master:9000/opt/data/wc/output7",
				"hdfs://hadoop-master:9000/opt/data/wc/output8" };

		int status = new TopKMapReduceV2().run(args);
		System.exit(status);
	}

}
