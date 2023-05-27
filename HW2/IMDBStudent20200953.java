import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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

public class IMDBStudent20200953 {
	public static class Movie_data {
		public String title;
		public double rating;
	
		public Info(String title, double rating) {
			this.title = title;
			this.rating = rating;
		}
		
		public String getTitle() {
			return this.title;
		}
	
		public double getRating() {
			return this.rating;
		}	
		public String getString() {
			return title + " " + rating;
		}
	}

	public staic class DoubleString implements WritableComparable {
		String joinKey = new String();
		String tableName = new String();

		public DoubleString() {}
		public DoubleString(String _joinKey, String _tableName) {
			joinKey = _joinKey;
			tableName = _tableName;
		}
	
		public void readFields(DataInput in) throws IOException {
			joinKey = in.readUTF();
			tableName = in.readUTF();
		}
	
		public void write(DataOutput out) throws IOException {
			out.writeUTF(joinKey);
			out.writeUTF(tableName);
		}
	
		public int compareTo(Object o1) {
			DoubleString o = (DoubleString) o1;
		
			int ret = joinKey.compareTo(o.joinKey);
			if (ret!=0) return ret;
			return tableName.compareTo(o.tableName);
		}
		public String toString() { return joinKey + " " + tableName; }

	}
	
	public static class CompositeKeyComparator extends WritableComparator {
		protected CompositeKeyComparator() {
			super(DoubleString.class, true);
		}

        	public int compare(WritableComparable w1, WritableComparable w2) {
            		DoubleString k1 = (DoubleString)w1;
            		DoubleString k2 = (DoubleString)w2;
            		
            		int result = k1.joinKey.compareTo(k2.joinKey);
            		if (0 == result) {
                		result = k1.tableName.compareTo(k2.tableName);
            		}
            		return result;
        	}

    	}

    	public static class FirstPartitioner extends Partitioner<DoubleString, Text> {
        	public int getPartition(DoubleString key, Text value, int numPartition) {
            		return key.joinKey.hashCode()%numPartition;
        	}
    	}

    	public static class FirstGroupingComparator extends WritableComparator {
        	protected FirstGroupingComparator() {
            		super(DoubleString.class, true);
        	}

        	public int compare(WritableComparable w1, WritableComparable w2) {
            		DoubleString k1 = (DoubleString)w1;
            		DoubleString k2 = (DoubleString)w2;
            		return k1.joinKey.compareTo(k2.joinKey);
        	}
    	}

	public static class AvgComparator implements Comparator<Movie_data> {
		public int compare(Movie_data x, Movie_data y) {
			if ( x.average > y.average ) 
				return 1;
			if ( x.average < y.average ) 
				return -1;
			return 0;
		}
	}
	
	public static void insertAvg(PriorityQueue q, String title, double rating, int topK) {
		Movie_data movie_data_head = (Movie_data) q.peek();
		if ( q.size() < topK || movie_data_head.rating < rating ) {
			Movie_data movie_data = new Movie_data(title, rating);
			q.add(movie_data);
			if(q.size() > topK) q.remove();
		}
	}
	
	public static class IMDBMapper extends Mapper<Object, Text, DoubleString, Text> {
		boolean movieFile = true;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] movie = value.toString().split("::");
			DoubleString outputKey = new DoubleString();
			Text outputValue = new Text();
			
			if(movieFile) {
				String id = movie[0];
				String title = movie[1];
				String genre = movie[2];
				
				StringTokenizer itr = new StringTokenizer(genre, "|");
				boolean isFantasy = false;
				while(itr.hasMoreTokens()) {
					if(itr.nextToken().equals("Fantasy")) {
						isFantasy = true;
						break;
					}
				}
				
				if(isFantasy) {
					outputKey = new DoubleString(id, "Movies");
					outputValue.set("Movies::" + title);
					context.write( outputKey, outputValue );
				}
			} else {
				String id = movie[1];
				String rate = movie[2];
				
				outputKey = new DoubleString(id, "Ratings::");
				outputValue.set("Ratings::" + movie_rate);
				context.write( outputKey, outputValue );
			}
			
			
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "movies.dat" ) != -1 ) movieFile = true;
			else movieFile = false;
		}
	}
	
	public static class IMDBReducer extends Reducer <DoubleString,Text,Text,DoubleWritable> {
		private PriorityQueue<Movie_data> queue;
		private Comparator<Movie_data> comp = new AvgComparator();
		private int topK;
		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String title = "";
			double total = 0;
			int i = 0;
			for(Text val : values) {
				String[] splt = val.toString().split("::");
				String file_type = splt[0];
				
				if(i == 0) {
					if(!file_type.equals("Movies")) 
						break;
					title = splt[1];
				} else {
					total += Double.parseDouble(splt[1]);
				}
				
				i++;
			}
			
			if (total != 0) {
				double avg = ((double) total) / (i - 1);
				insertAvg(queue, title, avg, topK);
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie_data>(topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Movie_data movie_data = (Movie_data) queue.remove();
				context.write(new Text(movie_data.getTitle()), new DoubleWritable(movie_data.getRating()));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: IMDBStudent20200953 <in> <out>");
			System.exit(2);
		}
		conf.setInt("topK", Integer.valueOf(otherArgs[2]));
		Job job = new Job(conf, "IMDBStudent20200953");
		job.setJarByClass(IMDBStudent20200953.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
