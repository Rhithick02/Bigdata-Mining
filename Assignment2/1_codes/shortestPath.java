package ass_1_q2;

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

public class shortestPath {
	static private final Path dir = new Path("shortestPath");
	static int fin_dist = Integer.MAX_VALUE, startPage, endPage;
	static String fin_path = "null";
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int cur_node = Integer.parseInt(value.toString().split("\t")[0]);
			String detail = value.toString().split("\t")[1];
			context.write(new IntWritable(cur_node), new Text(detail));
			int distance = Integer.parseInt(detail.split(",")[0]);
			if(distance != Integer.MAX_VALUE) {
				String path = detail.split(",")[1];
				String neighbours_string = detail.split(",")[2];
				if(!neighbours_string.equals("null")) {
					String[] neighbours = neighbours_string.split(" ");
					for(int i = 0; i < neighbours.length; i++) {
						int neighbour_id = Integer.parseInt(neighbours[i]);
						int neighbour_dist = distance + 1;
						String neighbour_path = path;
						if(path.equals("null")) {
							neighbour_path = Integer.toString(cur_node);
						} else {
							neighbour_path += " " + Integer.toString(cur_node);
						}
						String merge = neighbour_dist + "," + neighbour_path + ",null";
						context.write(new IntWritable(neighbour_id), new Text(merge));
					}
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int distance = Integer.MAX_VALUE;
			String neighbours = "null";
			String path = "null";
			for(Text it: values) {
				int dist = Integer.parseInt(it.toString().split(",")[0]);
				String temp_path = it.toString().split(",")[1];
				String temp_neighb = it.toString().split(",")[2];
				if(!temp_neighb.equals("null")) {
					neighbours = temp_neighb;
				}
				if(dist < distance) {
					distance = dist;
					path = temp_path;
				}
			}
			if(key.get() == endPage && distance != Integer.MAX_VALUE) {
				fin_dist = distance;
				fin_path = path;
			}
			String merge = Integer.toString(distance) + "," + path + "," + neighbours;
			context.write(key, new Text(merge));
		}
	}
	
	public static void createFile(String source, Path inputPath,Job job) throws Exception {
		int MAX = Integer.MAX_VALUE;
		
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
			if(Integer.parseInt(alines[0]) == startPage) { 
				out.write(alines[0]+"\t"+ 0 +",null,"+temp);
				out.newLine();	
			} else {
				out.write(alines[0]+"\t"+ MAX +",null,"+temp);
				out.newLine();
			}
		}
		in.close();
		out.close();
	}
	
	public static void main(String[] args) throws Exception {
		String source = args[0];
		startPage = Integer.parseInt(args[1]);
		endPage = Integer.parseInt(args[2]);
		int maxLoop = 5, fl = 0;
		
		FileSystem fs;
        Job job = null;
        
		for(int i = 0; i < maxLoop; i++) {
			Configuration conf = new Configuration();
			job = Job.getInstance(conf);
    		job.setJarByClass(shortestPath.class);
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
	        
	        if(fin_dist != Integer.MAX_VALUE) {
	        	fin_path += " " + Integer.toString(endPage);
	        	fin_path = fin_path.replaceAll(" ", "->");
	        	System.out.println("Found shortest path with a distance of " + fin_dist + " : " + fin_path);
	        	fl = 1;
	        	break;
	        }
		}
		if(fl == 0) {
			System.out.println("Exceeded maximum number of iteration - " + maxLoop);
		}
	}	
}