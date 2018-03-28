import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class MaxTemperatureSpark {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureSpark <input path> <output path>");
            System.exit(-1);
        }

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "MaxTemperatureSpark", conf);

        sc.textFile(args[0])
                .map(s -> s.split("\t"))
                .filter(rec -> !rec[1].equals("9999") && rec[2].matches("[01459]"))
                .mapToPair((strings -> new Tuple2<>(Integer.valueOf(strings[0]), Integer.valueOf(strings[1]))))
                .reduceByKey(Math::max)
                .saveAsTextFile(args[1]);
    }
}
