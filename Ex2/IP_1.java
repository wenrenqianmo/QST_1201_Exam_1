import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IP_1 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private HashSet<String> set = new HashSet<String>();
		
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String ip=line.split("\t")[0];
			set.add(ip);
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new Text(), new Text(set.size()+""));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
		}
	}	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf,"Exam_1_IP_1");
		job.setJarByClass(IP_1.class);
		job.setMapperClass(Map.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
