package inf583.exA_spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import scala.Tuple2;

public class ExA
{
    public static void main( String[] args )
    {
    	String inputFile = "mini.txt";
    	String outputFolder = "output";
    	// Create a Java Spark Context
    	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	// Load our input data.
    	JavaRDD<String> input = sc.textFile(inputFile);
    	
    	// Split up into words.
    	JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
    	
    	// Transform into pairs and count.
    	JavaPairRDD<String, Integer> tuples = words.mapToPair(word -> new Tuple2<>(word, 1));
    	JavaPairRDD<String, Integer> counts = tuples.reduceByKey((a,b) -> a+b);
    		
    	// Save the word count back out to an output folder.
    	counts.saveAsTextFile(outputFolder);
    }
}
