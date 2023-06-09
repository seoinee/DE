import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.Serializable;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;

public final class UBERStudent20200953 {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: UBERStudent20200953 <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("UBERStudent20200953")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaPairRDD<String, String> words = lines.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                StringTokenizer itr = new StringTokenizer(s, ",");
                String region = itr.nextToken();
                String tmpDate = itr.nextToken();
                String vehicles = itr.nextToken();
                String trips = itr.nextToken();

                itr = new StringTokenizer(tmpDate, "/");
                int month = Integer.parseInt(itr.nextToken());
                int day = Integer.parseInt(itr.nextToken());
                int year = Integer.parseInt(itr.nextToken());

                String[] days = {"MON", "TUE", "WED", "THR", "FRI", "SAT", "SUN"};
                LocalDate date = LocalDate.of(year, month, day);
                DayOfWeek tmpDayOfWeek = date.getDayOfWeek();
                int dayOfWeekNumber = tmpDayOfWeek.getValue();
                String dayOfWeek = days[dayOfWeekNumber - 1];

                String key = region + "," + dayOfWeek;
                String value = trips + "," + vehicles;

                return new Tuple2(key, value);
            }
        });

        JavaPairRDD<String, String> counts = words.reduceByKey(new Function2<String, String, String>() {
            public String call(String val1, String val2) {
                String[] val1_splt = val1.split(",");
                String[] val2_splt = val2.split(",");

                int trips = Integer.parseInt(val1_splt[0]) + Integer.parseInt(val2_splt[0]);
                int vehicles = Integer.parseInt(val1_splt[1]) + Integer.parseInt(val2_splt[1]);

                return trips + "," + vehicles;
            }
        });

        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
