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
		private Text region_day = new Text();
		private Text trips_vehicles = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String region = itr.nextToken();
			String date = itr.nextToken();
			String vehicles = itr.nextToken();
        		String trips = itr.nextToken();
			
			String[] days = {"MON", "TUE", "WED", "THR", "FRI", "SAT", "SUN"};
			
			itr = new StringTokenizer(date, "/");
			int month = Integer.parseInt(itr.nextToken());
			int day = Integer.parseInt(itr.nextToken());
			int year = Integer.parseInt(itr.nextToken());

			LocalDate date2 = LocalDate.of(year, month, day);
			DayOfWeek dayOfWeek = date2.getDayOfWeek();
			int dayOfWeekNumber = dayOfWeek.getValue();
		       String dayStr = days[dayOfWeekNumber - 1];

			region_day.set(region + "," + dayStr);
			trips_vehicles.set(trips + "," + vehicles);
			context.write(region_day, trips_vehicles);
		}
	}

	public static class UBERReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			long tripsRslt = 0;
			long vehiclesRslt = 0;

			for (Text val : values) {
				String tv = val.toString();
				StringTokenizer itr = new StringTokenizer(tv, ",");
				tripsRslt += Long.parseLong(itr.nextToken());
				vehiclesRslt += Long.parseLong(itr.nextToken());
			}
			result.set(tripsRslt + "," + vehiclesRslt);
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

