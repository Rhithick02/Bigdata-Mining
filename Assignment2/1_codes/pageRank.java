package ass_2_q2;

import java.io.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class pageRank {
	static private final Path dir = new Path("pageRank");
	private static double d = 0.15;
	private static int tot_nodes = 6012;
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			String value = val.toString();
			
			int cur_node = Integer.parseInt(value.split("\t")[0]);
			String detail = value.split("\t")[1];
			
			context.write(new IntWritable(cur_node), new Text(detail));
			
			double page_rank = Double.parseDouble(detail.split(",")[0]);
			String temp = detail.split(",")[1];
			if(!temp.equals("null")) {
				String[] neighbours = temp.split(" ");
				int n = neighbours.length;	
				float outpage_rank = (float) page_rank / n;
				for(int i = 0; i < n; i++) {
					int node = Integer.parseInt(neighbours[i]);
					String merge = outpage_rank + ",null";
					context.write(new IntWritable(node), new Text(merge));
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> val, Context context) throws IOException, InterruptedException {
			double sum = 0;
			String neighbours = "null";
			for(Text it : val) {
				String value = it.toString();
				String temp_neighbour = value.split(",")[1];
				if(!temp_neighbour.equals("null")) {
					neighbours = temp_neighbour;
				} else {
					sum += Double.parseDouble(value.split(",")[0]);
				}
			}
			double page_rank = 1.0 * (1 - d) / tot_nodes + 1.0 * d * sum;
			page_rank = 1.0 * Math.round(page_rank * 100000) / 100000; 
			String merge = String.format("%f", page_rank) + "," + neighbours;
			context.write(key, new Text(merge));
		}
	}
	
	public static void createFile(String source, Path inputPath,Job job) throws Exception {
		File fin = new File(source);		
		FileInputStream fis = new FileInputStream(fin);
		BufferedReader in = new BufferedReader(new InputStreamReader(fis));
		final FileSystem fs = FileSystem.get(job.getConfiguration());
		OutputStreamWriter fstream = new OutputStreamWriter(fs.create(inputPath,true));
		BufferedWriter out = new BufferedWriter(fstream);
		
		String aLine = null;
		while ((aLine = in.readLine()) != null) {
			String[] alines = aLine.split("\t");
			String[] splt = alines[1].split(",");
			String temp = "";
			int n = splt.length;
			for(int i = 0; i < n; i++) {
				temp += splt[i];
				if(i != n-1) temp += " ";
			}
			double rank_init = 1.0 / tot_nodes;
			out.write(alines[0] + "\t" + rank_init + "," + temp);
			out.newLine();
		}
		in.close();
		out.close();
	}
	
	public static void main(String[] args) throws Exception {
		String source = args[0];
		int maxLoop = 5;
		
		FileSystem fs;
        Job job = null;
        
		for(int i = 0; i < maxLoop; i++) {
			Configuration conf = new Configuration();
			job = Job.getInstance(conf);
    		job.setJarByClass(pageRank.class);
    		if(i == 0) {
    			Path Mapfile = new Path(dir + "/input");
            	FileInputFormat.setInputPaths(job, Mapfile);
            	createFile(source, Mapfile, job);
    		} else {
    			FileInputFormat.setInputPaths(job, new Path(dir + "/output/0"+i));
    		}
    		if(i > 1) {
                fs = FileSystem.get(job.getConfiguration());
                fs.delete(new Path(dir + "/output/0"+(i-1)), true);
    		}
    		
	        FileOutputFormat.setOutputPath(job, new Path(dir + "/output/0"+(i+1)));	        
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setMapperClass(Map.class);
	        job.setMapOutputKeyClass(IntWritable.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setReducerClass(Reduce.class);
	        job.setOutputKeyClass(IntWritable.class);
	        job.setOutputValueClass(Text.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        job.waitForCompletion(true);
		}
		System.out.println("Total number of iterations : " + maxLoop);
	}
}
