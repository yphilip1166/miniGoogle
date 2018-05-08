import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public class PageRankSpark {
    private static final Pattern SPACES = Pattern.compile(",");

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String[] args) throws Exception {
        String inputFile="data/input.txt";
        int numIter = 1;
        if (args.length >= 2) {
            inputFile = args[0];
            numIter=Integer.parseInt(args[1]);
        }
        inputFile = "data/graph2.csv";
        //inputFile = "data/input.txt";
        numIter = 10;

        SparkSession spark = SparkSession
                .builder()
                .config(new SparkConf().setAppName("JavaPageRank").setMaster("local"))
                .getOrCreate();

        // Loads in input file. It should be in format of:
        //     URL, neighbor URL
        //     URL, neighbor URL
        //     URL, neighbor URL
        //     ...
        JavaRDD<String> lines = spark.read().textFile(inputFile).javaRDD();

        // Loads all URLs from input file and initialize their neighbors.
        // links stores the the graph by adjacency list
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
            String[] parts = SPACES.split(s);
            if(parts.length == 2)
                return new Tuple2<>(parts[0], parts[1]);
            else
                return new Tuple2<>("f", "f");
        }).distinct().groupByKey().cache();

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        // ranks store page rank for all urls

        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < numIter; current++) {
            // Calculates URL contributions to the rank of other URLs.
            //links.join(ranks).values().rdd().saveAsTextFile("rdd"+String.valueOf(current)+".txt");
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1) {
                            results.add(new Tuple2<>(n, s._2() / urlCount));
                        }
                        return results.iterator();
                    });

            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
        }

        PrintWriter writer = new PrintWriter("data/output2.txt", "UTF-8");

        // Collects all URL ranks and dump them to console.
        List<Tuple2<String, Double>> output = ranks.collect();
        for (Tuple2<?,?> tuple : output) {
            writer.println(tuple._1() + ", " + tuple._2());
        }
        writer.close();

        spark.stop();
    }
}
