package inf583.exA_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import scala.Tuple2;

public class Ex1
{
    public static void main( String[] args )
    {
    	String inputFile = "integers.txt";
    	String outputFolder = "output1";

    	// Create a Java Spark Context
    	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("exA_spark");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	// Load our input data.
    	JavaRDD<String> input = sc.textFile(inputFile);

		// Transform into integers with the same key of 1
		JavaPairRDD<Integer,Integer> integers = input.mapToPair(s -> new Tuple2<Integer, Integer>(1, Integer.parseInt(s)));

    	// Reduce by keeping the largest value
		JavaPairRDD<Integer,Integer> max = integers.reduceByKey((a,b) -> (a < b ? b : a));

		JavaRDD<Integer> result = max.values();

		System.out.println(result.collect());
    	result.saveAsTextFile(outputFolder);
    }
}
