package inf583.exA_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Ex2
{
    public static void main( String[] args )
    {
    	String inputFile = "integers.txt";
    	String outputFolder = "output2";

    	// Create a Java Spark Context
    	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("exA_spark");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	// Load our input data.
    	JavaRDD<String> input = sc.textFile(inputFile);

		// Transform into a pair of double (the number) and integer (the coefficient) with the same key of 1
		JavaPairRDD<Integer,Tuple2<Double,Integer>> integers = input.mapToPair(s -> new Tuple2<Integer, Tuple2<Double, Integer>>(1, new Tuple2<Double,Integer>(Double.parseDouble(s),1)));

    	// Reduce by summing the numbers and the coefficients
		JavaPairRDD<Integer,Tuple2<Double, Integer>> sumAndCoeff = integers.reduceByKey((a,b) ->  new Tuple2<Double, Integer>(a._1+b._1, a._2+b._2 )  );

		//we divide the sum by the coefficient
		JavaRDD<Double>  result = sumAndCoeff.map(s-> s._2._1/s._2._2);

		System.out.println(result.collect());
    	result.saveAsTextFile(outputFolder);
    }
}
