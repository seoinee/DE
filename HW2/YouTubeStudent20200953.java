import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20200953 {
	public static class Video {
		public String category;
		public double rating;
	
		public Video(String category, double rating) {
			this.category = category;
			this.rating = rating;
		}
	
		public String getCategory() {
			return this.category;
		}
	
		public double getRating() {
			return this.rating;
		}	
	}


	public static class AverageComparator implements Comparator<Video> {
		public int compare(Video v1, Video v2) {
			if ( v1.average > v2.average ) 
				return 1;
			if ( v1.average < v2.average ) 
				return -1;
			return 0;
		}
	}
	
	public static void insertVideo(PriorityQueue q, String category, double rating, int topK) {
		Video video_head = (Video) q.peek();
		if (q.size() < topK || video_head.rating < rating) {
			Video video = new Video(category, rating);
			q.add(video);
			
			if(q.size() > topK) q.remove();
		}
		
	}
	
	public static class YoutubeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splt = value.toString().split("\\|");
            		String category = splt[3];
            		double rating = Double.parseDouble(splt[6]);

            		context.write(new Text(category), new DoubleWritable(rating));
		}
	}
	
	public static class YoutubeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private PriorityQueue<Video> queue;
		private Comparator<Video> comp = new AverageComparator();
		private int topK;
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double total = 0;
			int i = 0;
			
			for(DoubleWritable val : values) {
				total += val.get();
				i++;
			}
			
			double average = total / (double) i;
			
			insertVideo(queue, key.toString(), average, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Video>(topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Video video = (Video) queue.remove();
				context.write(new Text(video.getCategory()), new DoubleWritable(video.getRating()));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: YouTubeStudent20200953 <in> <out>");
			System.exit(2);
		}
		conf.setInt("topK", Integer.valueOf(otherArgs[2]));
		Job job = new Job(conf, "YouTubeStudent20200953");
		job.setJarByClass(YouTubeStudent20200953.class);
		job.setMapperClass(YoutubeMapper.class);
		job.setReducerClass(YoutubeReducer.class);	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
