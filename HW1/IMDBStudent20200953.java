import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20200953 {
	public static class IMDBMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			String[] token = s.split("::");
			StringTokenizer itr = new StringTokenizer(token[token.length-1],"|");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken()); 
				context.write (word, one);
			}
		}
	}
	public static class IMDBReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable output = new IntWritable();
		public void reduce(Text key, Iterable‹IntWritable> values, Context context) throws I0Exception, InterruptedException {
			int sum = 0;			
			for (IntWritable v : values) {
				sum += v.get();
			}
			output.set(sum);
			context.write(key, output);
		}
	}
	public static void main (String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: IMDB <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "IMDB");
		job.setJarByClass(IMDBStudent20200953.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);
	}
}
