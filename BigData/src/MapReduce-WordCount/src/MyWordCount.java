import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * MapReduce example
 */
public class MyWordCount {

	// Mapper
	/*
	 * Word Count Mapper Class
	 */
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// text input
			String lineValue = value.toString();
			StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
			while (stringTokenizer.hasMoreElements()) {
				String wordValue = stringTokenizer.nextToken();
				word.set(wordValue);
				context.write(word, one);
			}
		}
		

	}

	// Reducer
	/*
	 * Word Count Reducer Class
	 */
	static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// client
	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * PriviledgedActionException
	 * FileUtil
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		args = new String[]{
				"hdfs://hadoop-master:9000/opt/data/wc/input/",
				"hdfs://hadoop-master:9000/opt/data/wc/output7/"};
		
		
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "wc");

		// 1.设置Job运行Class
		job.setJarByClass(MyWordCount.class);

		// 2设置 Job 的Mapper & Reducer
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		// 3.设置Job的输入输出目录
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 4. 输出结果的key & value 类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 5. 提交Job，等待运行结果
		boolean isSuccess = job.waitForCompletion(true);
		String info = isSuccess ? "Success" : "Failed";
		System.out.println(info);
		System.exit(isSuccess ? 0 : 1);

	}
}
