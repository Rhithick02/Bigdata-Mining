import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class wordcount {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text val, Context context) 
			throws IOException, InterruptedException {
			
			String cur = val.toString();
			int n = cur.length();
			String temp = "";
			for(int i = 0; i < n; i++) {
				char ch = cur.charAt(i);
				if((ch>=65 && ch<=90) || (ch>=97 && ch<=122)) {
					temp += ch;
				}
				else if(ch == '.' || ch == ' ' || ch == ','){
					if(temp != "") {
						context.write(new Text(temp), new IntWritable(1));
						temp = "";
					}					
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> val, Context context)
			throws IOException, InterruptedException {
			
			int sum = 0;
			for(IntWritable it: val) {
				sum += it.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "wordcount");
		
		job.setJarByClass(wordcount.class);
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
