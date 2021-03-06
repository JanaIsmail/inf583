package inf583.exB_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import java.io.Serializable;
import java.util.*;

import scala.Tuple2;

public class EigenVectorCentrality implements Serializable
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

		// Transform to pairs of node and list of neighbors
		JavaPairRDD<Integer,List<String>> adjacency = input.mapToPair(s -> {

			List<String> line = new ArrayList<String>(Arrays.asList(s.split(" ")));

			Integer entry =  Integer.parseInt(line.get(0));
			line.remove(0);
			return new Tuple2<Integer, List<String>>(entry, line);
		} );

		// Flatten the rdd to have and rdd of edges
		JavaPairRDD<Integer,String> edges_string = adjacency.flatMapValues(s-> s);

		// Parse the strings into integers
		JavaPairRDD<Integer,Integer> edges  = edges_string.mapValues(Integer::parseInt) ;

		// Load the list of nodes
		JavaRDD<String> input_idlabels = sc.textFile("idslabels.txt");

		// Create the initial uniform vector
		JavaPairRDD<Integer,Double> vector =  input_idlabels.mapToPair( s -> new Tuple2<Integer,Double>( Integer.parseInt(s.split(" ")[0]), 1.0/64375) );

		// Reverse the rdd of edges to make the join
		JavaPairRDD<Integer,Integer> edges_reverse  = edges.mapToPair(s -> new Tuple2<Integer,Integer>(s._2, s._1));


		int converge = 0;
		while(converge < 9) {

			// Join the reversed edges and the vector
			JavaPairRDD<Integer, Tuple2<Integer, Double>> vectorjoined_rev = edges_reverse.join(vector);

			// Keep the node id and the value of the vector
			JavaPairRDD<Integer, Double> vectorjoined = vectorjoined_rev.mapToPair(s -> new Tuple2<Integer, Double>(s._2._1, s._2._2));

			// Reduce by summing the values of the vector that correspond to the same node id
			JavaPairRDD<Integer, Double> vectorreduced = vectorjoined.reduceByKey((a, b) -> a + b);

			// Calculate the norm of the vector
			double norm = 0;
			List<Double> list = new ArrayList<Double>(vectorreduced.values().collect());
			for (double i : list) {
				norm += i * i;
			}
			norm = Math.sqrt(norm);
			final double n = norm;

			// Normalize the vector
			vector = vectorreduced.mapToPair(s -> new Tuple2<Integer, Double>(s._1, s._2 / n));
			converge++;
		}

		//vector.foreach(data -> System.out.println(data._1 + " " + data ._2));

		// Get the max value
		Map<Integer, Double> vectorAsMap = new HashMap<Integer, Double>(vector.collectAsMap());
		Map.Entry<Integer, Double> maxEntry = null;
		for (Map.Entry<Integer, Double> entry : vectorAsMap.entrySet()) {
			if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
				maxEntry = entry;
			}
		}

		System.out.println(maxEntry.toString());

		// Get the pages names
		JavaPairRDD<Integer, String> labels = input_idlabels.mapToPair(s -> new Tuple2<Integer,String>(Integer.parseInt(s.split(" ")[0]), s.split(" ", 2)[1]));

		// Get the most important page
		String page = labels.collectAsMap().get(maxEntry.getKey());

		System.out.println("The most important page in Wikipedia is " + page);

    	//vector.saveAsTextFile(outputFolder);
    }
}
