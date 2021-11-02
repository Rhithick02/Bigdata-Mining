package ass_3_q1;

import java.io.*;
import java.util.*;
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

public class worldwide {
	static int k = 0;
	static String st;
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text val, Context context) 
			throws IOException, InterruptedException {
			
			String cur_line = val.toString();
			String[] words = cur_line.split(",");
			if(words[2].equals("TMAX")) {
				k = Integer.valueOf(words[3]);
				st = words[0];
			} else if(words[2].equals("TMIN") && words[0].equals(st)) {
				int temp = Integer.valueOf(words[3]);
				context.write(new Text(words[1]), new IntWritable(k-temp));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterable<IntWritable> val, Context context)
			throws IOException, InterruptedException {
			int valid = 0;
			double variation = 0;
			for(IntWritable it: val) {
				variation += 1.0 * it.get();
				valid++;
			}
			System.out.println(variation);
			variation = 1.0 * variation / (valid * 10);
			System.out.println(valid);
			float ans = (float) variation;
			context.write(key, new FloatWritable(ans));
		}			
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "worldwide");
		
		job.setJarByClass(worldwide.class);
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

