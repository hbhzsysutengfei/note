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

/*
 * 手机流量分析
 */
public class DataTotalMapReduce {

	// Map class
	static class DataTotalMapper extends
			Mapper<LongWritable, Text, Text, DataWritable> {
		private DataWritable dataWritable = new DataWritable();
		private Text mapOutputKey = new Text();
		
		@Override
		public  void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			
			//split
			String[] strs = lineValue.split("\t");
			
			//get data
			String phoneNum = strs[1];
			int upPackNum = Integer.valueOf(strs[6]);
			int upPayLoad = Integer.valueOf(strs[7]);
			int downPackNum = Integer.valueOf(strs[8]);
			int downPayLoad = Integer.valueOf(strs[9]);
			
			//set map output
			mapOutputKey.set(phoneNum);
			dataWritable.set(upPackNum, upPayLoad, downPackNum, downPayLoad);
			
			context.write(mapOutputKey, dataWritable);
		}
	}

	// Reduce class
	static class DataTotalReducer extends
			Reducer<Text, DataWritable, Text, DataWritable> {
		private DataWritable dataWritable = new DataWritable();
		
		@Override
		public  void reduce(Text key, Iterable<DataWritable> values,
				Context context) throws IOException, InterruptedException {
			int upPackNum = 0;
			int upPayLoad = 0;
			int downPackNum = 0;
			int downPayLoad =0;
			
			for(DataWritable data: values){
				upPackNum += data.getUpPackNum();
				upPayLoad += data.getUpPayLoad();
				downPackNum += data.getDownPackNum();
				downPayLoad += data.getDownPayLoad();
			}
			
			//set datawritable
			dataWritable.set(upPackNum, upPayLoad, downPackNum, downPayLoad);
			
			//set reduce/job output
			context.write(key, dataWritable);
		}

	}

	// dirver code
	
	public int run(String[] args) throws Exception{
		//get conf
		Configuration conf = new Configuration();
		
		//create job
		Job job = new Job(conf,DataTotalMapReduce.class.getSimpleName());
		
		//set job
		job.setJarByClass(DataTotalMapReduce.class);
		//job input
		Path inputDirPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputDirPath);
		//job map class
		job.setMapperClass(DataTotalMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataWritable.class);
		
		//job reduce class
		job.setReducerClass(DataTotalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataWritable.class);
		
		
		//job output
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		
		
		//submit job 
		boolean isSuccess = job.waitForCompletion(true);
		
		
		return isSuccess?0:1;
		
	}
	public static void main(String[] args) throws Exception {
		args = new String[]{
				"hdfs://hadoop-master:9000/opt/data/wc/input",
				"hdfs://hadoop-master:9000/opt/data/wc/output"
		};
		
		int status = new DataTotalMapReduce().run(args);
		System.exit(status);
	}
}
