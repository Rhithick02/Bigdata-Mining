package ass_2_q1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class calctemp {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text val, Context context) 
			throws IOException, InterruptedException {
			
			String cur_line = val.toString();
			String[] words = cur_line.split(",");
			int temp;
			if(words[0].equals("USW00094728") && (words[2].equals("TMAX") || words[2].equals("TMIN"))) {
				temp = Integer.valueOf(words[3]);
				context.write(new Text(words[0]+"-"+words[1]), new IntWritable(temp));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterable<IntWritable> val, Context context)
			throws IOException, InterruptedException {
		
			float dif;
//			int cnt = 0;
			int mini = Integer.MAX_VALUE;
			int maxi = Integer.MIN_VALUE;
			for(IntWritable it: val) {
//				cnt++;
				mini = Math.min(mini, it.get());
				maxi = Math.max(maxi, it.get());
			}
			dif = (float) (maxi - mini) / 10;
			context.write(key, new FloatWritable(dif));
//			int cnt = 0;
//			for(IntWritable it: val) {
//				cnt += it.get();
//			}
//			context.write(key, new IntWritable(cnt));
		}			
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "calctemp");
		
		job.setJarByClass(calctemp.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
//		Path output_path = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

