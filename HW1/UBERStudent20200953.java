import java.io.*;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate; 

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20200953 {

	public static class UBERMapper extends Mapper<Object, Text, Text, Text> {
		private Text regionDay = new Text();
		private Text tripsVehicles = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String region = itr.nextToken();
			String date = itr.nextToken();
			
			itr2 = new StringTokenizer(date, "/");
			int month = Integer.parseInt(itr2.nextToken());
			int day = Integer.parseInt(itr2.nextToken());
			int year = Integer.parseInt(itr2.nextToken());
			
			String[] days = {"MON", "TUE", "WED", "THR", "FRI", "SAT", "SUN"};
			LocalDate dt = LocalDate.of(year, month, day);
			DayOfWeek dayOfWeek = dt.getDayOfWeek();
			int dayOfWeekNumber = dayOfWeek.getValue();
			String d = days[dayOfWeekNumber - 1];
			
			String vehicles = itr.nextToken();
        		String trips = itr.nextToken();

			regionDay.set(region + "," + d);
			tripsVehicles.set(trips + "," + vehicles);
			context.write(regionDay, tripsVehicles);
		}
	}

	public static class UBERReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			private long tripsVal= 0;
			private long vehiclesVal = 0;
			for (Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				tripsVal += Long.parseLong(itr.nextToken());
				vehiclesVal += Long.parseLong(itr.nextToken());
			}
			result.set(tripsVal + "," + vehiclesVal);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UBER <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBERStudent20200953");
		job.setJarByClass(UBERStudent20200953.class);
		job.setMapperClass(UBERMapper.class);
		job.setCombinerClass(UBERReducer.class);
		job.setReducerClass(UBERReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
