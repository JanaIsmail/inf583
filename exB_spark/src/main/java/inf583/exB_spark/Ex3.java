package inf583.exA_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Ex3
{
    public static void main( String[] args )
    {
    	String inputFile = "integers.txt";
    	String outputFolder = "output3";

    	// Create a Java Spark Context
    	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("exA_spark");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	// Load our input data.
    	JavaRDD<String> input = sc.textFile(inputFile);

		// We map every number as a key with the same value of 1
		JavaPairRDD<Integer,Integer> integers = input.mapToPair(s -> new Tuple2<Integer, Integer>(Integer.parseInt(s) ,1)  );

    	// Reduce by summing the numbers and the coefficients
		JavaRDD<Integer> result = integers.reduceByKey((a,b)->1).keys() ;


		System.out.println(result.collect());
    	result.saveAsTextFile(outputFolder);
    }
}
