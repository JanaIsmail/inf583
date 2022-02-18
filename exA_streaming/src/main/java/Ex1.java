import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

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

public class Ex1 {

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
    JavaPairDStream<Integer,Integer> integers = stream.mapToPair(s -> new Tuple2<Integer, Integer>(1, Integer.parseInt(s)));

    JavaPairDStream<Integer,Integer> result = integers.reduceByKeyAndWindow((a,b) -> (a<b ? b: a), Durations.seconds(180), Durations.seconds(60));


    result.foreachRDD( x-> { x.collect().stream().forEach(n-> System.out.println(n));});
    
    jssc.start();
    try {jssc.awaitTermination();} catch (InterruptedException e) {e.printStackTrace();}
  }
}

  

