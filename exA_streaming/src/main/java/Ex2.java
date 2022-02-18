import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class Ex2 {

  public static void main(String[] args) throws IOException {

  
  	Logger.getLogger("org").setLevel(Level.ERROR);
  	Logger.getLogger("akka").setLevel(Level.ERROR);


    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("ExA");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));

    JavaSparkContext jsc = new JavaSparkContext(jssc.ssc().sc()) ;

    FileReader fr = new FileReader(new File("integers.txt"));


    BufferedReader br = new BufferedReader(fr);
    String line ;
    int count = 0;
    ArrayList<String> batch = new ArrayList<String>();
    Queue<JavaRDD<String>> rdds = new LinkedList<>();
    while ((line = br.readLine ()) != null){
      count +=1;
      if ( count == 10)
      {
        JavaRDD<String> rdd = jsc.parallelize(batch);
        rdds.add(rdd);
        batch = new ArrayList<String>();
        count = 0;
      }
      batch.add(line);
    }
    JavaRDD<String>rdd = jsc.parallelize(batch);
    rdds.add(rdd);
    JavaDStream<String> stream = jssc.queueStream(rdds,true);


    //JavaDStream<Integer> integers = stream.map(s -> Integer.parseInt(s));
    JavaPairDStream<Integer,Tuple2<Double,Integer>> integers = stream.mapToPair(s -> new Tuple2<Integer, Tuple2<Double,Integer>>(1, new Tuple2<Double,Integer>( Double.parseDouble(s), 1)  ));

    JavaPairDStream<Integer, Tuple2<Double,Integer>> sumAndCoeff = integers.reduceByKeyAndWindow((a,b) ->  new Tuple2<Double, Integer>(a._1+b._1 , a._2+b._2 )  , Durations.seconds(180), Durations.seconds(60));

    JavaDStream<Double> result = sumAndCoeff.map(s ->  s._2._1/s._2._2);


    result.foreachRDD( x-> { x.collect().stream().forEach(n-> System.out.println(n));});
    
    jssc.start();
    try {jssc.awaitTermination();} catch (InterruptedException e) {e.printStackTrace();}
  }
}

  

