import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20200953 {

    public static class Youtube {
        public String category;
        public double rating;

        public Youtube(String category, double rating) {
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

    public static class YoutubeComparator implements Comparator<Youtube> {
        public int compare(Youtube y1, Youtube y2) {
            if (y1.rating > y2.rating) {
                return 1;
            } else if (y1.rating < y2.rating) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    public static void insertYoutube(PriorityQueue q, String category, double rating, int topK) {
        Youtube youtube_head = (Youtube) q.peek();
        if (q.size() < topK || youtube_head.rating < rating) {
            Youtube youtube = new Youtube(category, rating);
            q.add(youtube);
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
        private PriorityQueue<Youtube> queue;
        private Comparator<Youtube> comp = new YoutubeComparator();
        private int topK;
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double total = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                total += val.get();
                count++;
            }
            double average = total / (double) count;

            insertYoutube(queue, key.toString(), average, topK);
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            topK = conf.getInt("topK", -1);
            queue = new PriorityQueue<Youtube>(topK, comp);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            while( queue.size() != 0 ) {
                Youtube youtube = (Youtube) queue.remove();
                context.write( new Text(youtube.getCategory()), new DoubleWritable(youtube.getRating()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: YoutubeStudent20200953 <in> <out>");
            System.exit(2);
        }
        conf.setInt("topK", Integer.parseInt(otherArgs[2]));
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
