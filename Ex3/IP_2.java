import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IP_2 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private HashSet<String> set1 = new HashSet<String>();
		private HashSet<String> set2 = new HashSet<String>();
		private HashSet<String> set3 = new HashSet<String>();
		String inputName;
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String ip=line.split("\t")[0];
			String time=line.split("\t")[1];
			inputName = ((FileSplit) context.getInputSplit()).getPath().getName();
			if(inputName.equals("ip_time")){
				set1.add(ip);
			}else if(inputName.equals("ip_time_2")){
				set2.add(ip);
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			for(String str : set1){
				if(set2.contains(str)){
					set3.add(str);
				}
			}
			context.write(new Text(inputName), new Text(set3.size()+""));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
		}
	}	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf,"M/R_Dev");
		job.setJarByClass(IP_2.class);
		job.setMapperClass(Map.class);
		//job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(2);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
