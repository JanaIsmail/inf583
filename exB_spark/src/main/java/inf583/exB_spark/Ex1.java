package inf583.exB_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

public class Ex1
{
    public static void main( String[] args )
    {
    	String inputFile = "edgelist.txt";
    	String outputFolder = "output";

    	// Create a Java Spark Context
    	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("exB_spark");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	// Load our input data.
    	JavaRDD<String> input = sc.textFile(inputFile);

		// Transform into integers with the same key of 1
		JavaPairRDD<Integer,List<String>> integers = input.mapToPair(s -> {

			List<String> line = new ArrayList<String>(Arrays.asList(s.split(" ")));

			Integer entry =  Integer.parseInt(line.get(0));
			line.remove(0);
			return new Tuple2<Integer, List<String>>(entry, line);
		} );

		JavaPairRDD<Integer,String> adjacency2 = integers.flatMapValues(s-> s);

		JavaPairRDD<Integer,Integer> adjacency  = adjacency2.mapValues(Integer::parseInt) ;




		JavaRDD<String> input_idlabels = sc.textFile("idslabels.txt");

		JavaPairRDD<Integer,Double> vector =  input_idlabels.mapToPair( s -> new Tuple2<Integer,Double>( Integer.parseInt(s.split(" ")[0]), 1.0/64375) );

		JavaPairRDD<Integer,Integer> adjacency_reverse  = adjacency.mapToPair(s -> new Tuple2<Integer,Integer>(s._2, s._1));


		int converge = 0;
		while(converge <10) {
			JavaPairRDD<Integer, Tuple2<Integer, Double>> vectorjoined_rev = adjacency_reverse.join(vector);
			JavaPairRDD<Integer, Double> vectorjoined = vectorjoined_rev.mapToPair(s -> new Tuple2<Integer, Double>(s._2._1, s._2._2));
			JavaPairRDD<Integer, Double> vectorreduced = vectorjoined.reduceByKey((a, b) -> a + b);
			double norm = 0;
			List<Double> list = new ArrayList<Double>(vectorreduced.values().collect());

			for (double i : list) {
				norm += i * i;
			}
			norm = Math.sqrt(norm);
			final double n = norm;
			vector = vectorreduced.mapToPair(s -> new Tuple2<Integer, Double>(s._1, s._2 / n));
			converge++;
		}


		vector.foreach(data -> System.out.println(data._1 + " " + data ._2));

		// Reduce by keeping the largest value
//		JavaPairRDD<Integer,Integer> max = integers.reduceByKey(
//				(a,b) -> {
//
//
//					return new Tuple2<Integer,Integer>();
//				});

		//JavaRDD<Integer> result = max.values();

		//System.out.println(result.collect());
    	//result.saveAsTextFile(outputFolder);
    }

}
