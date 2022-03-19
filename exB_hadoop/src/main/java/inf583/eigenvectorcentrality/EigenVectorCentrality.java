package inf583.eigenvectorcentrality;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class EigenVectorCentrality {

  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable,Text>{
    private final static Text node = new Text();
    private final static IntWritable edge = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(" ");
      node.set(line[0]);
      for(int i =1 ;  i<line.length ;  i++ ){
        edge.set(Integer.parseInt(line[i]));
        context.write(edge,node);
      }
    }
  }

  public static class IntSumReducer extends Reducer<IntWritable,Text, IntWritable,Text> {
    private  Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String line = new String();
      for(Text i : values){
        line += i.toString() + " ";
      }
      line += Double.toString(1.0 / 64735);
      result.set(line);
      context.write(key, result);
    }
  }


  public static class TokenizerMapper2 extends Mapper<Object, Text, IntWritable,Text>{
    private final static IntWritable node = new IntWritable();
    private final static Text edge = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(" ");
      edge.set(line[0]);
      Text vect = new Text();
      vect.set("_" + line[line.length-1]);
      for(int i =1 ;  i<line.length-2;  i++ ){
        node.set(Integer.parseInt(line[i]));
        context.write(node,edge);
        context.write(node,vect);
      }

    }
  }

  public static class IntSumReducer2 extends Reducer<IntWritable,Text, IntWritable,Text> {
    private  Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String edges = new String();
      double vect = 0.0;
      for(Text i : values){
        if(i.toString().charAt(0) =='_'){
          vect += Double.parseDouble(i.toString().split("_")[1]);
        }
        else{
          edges += i.toString() + " ";
        }
      }
      result.set(edges  + vect);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "eigenvector");
    job.setJarByClass(EigenVectorCentrality.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path("input"));
    FileOutputFormat.setOutputPath(job, new Path("output"));
    job.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "eigenvector");
    job2.setJarByClass(EigenVectorCentrality.class);
    job2.setMapperClass(TokenizerMapper2.class);
    job2.setCombinerClass(IntSumReducer2.class);
    job2.setReducerClass(IntSumReducer2.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("output"));
    FileOutputFormat.setOutputPath(job2, new Path("output2"));
    job2.waitForCompletion(true);


  }
}