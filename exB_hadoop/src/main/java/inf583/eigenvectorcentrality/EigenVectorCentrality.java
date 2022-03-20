package inf583.eigenvectorcentrality;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class EigenVectorCentrality {

  public static class EdgeMapper extends Mapper<Object, Text, IntWritable,Text>{
    private final static Text node = new Text();
    private final static IntWritable dest = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(" ");
      node.set(line[0]);
      for(int i = 1; i < line.length; i++ ){
        dest.set(Integer.parseInt(line[i]));
        context.write(dest,node);
      }
    }
  }

  public static class VectMapper extends Mapper<Object, Text, IntWritable,Text>{
    private final static IntWritable node = new IntWritable();
    private final static Text val = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(" |\t");
      node.set(Integer.parseInt(line[0]));
      val.set(line[1]);
      context.write(node,val);
    }
  }

  public static class MultReducer extends Reducer<IntWritable,Text, IntWritable,Text> {
    private  Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String line = "";
      String vect = "";
      for(Text i : values){
        if(i.toString().contains(".")){
          vect = i.toString();
        }
        else{
          line += i.toString() + " ";
        }
      }
      line += vect;
      line += " 0.0";
      result.set(line);
      context.write(key, result);
    }
  }


  public static class ResultMapper extends Mapper<Object, Text, IntWritable,Text> {
    private final static IntWritable node = new IntWritable();
    private final static Text val = new Text();
    private final static Text dest = new Text();

    private final static IntWritable dest_int = new IntWritable();
    private final static Text node_text = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(" |\t");
      dest.set(line[0]);
      dest_int.set(Integer.parseInt(line[0]));
      val.set("_" + line[line.length-2]);
      for(int i = 1; i < line.length - 2; i++ ){
        if(line[i].length() == 1 || line[i].charAt(1) != '.') {
          node.set(Integer.parseInt(line[i]));
          node_text.set(line[i]);
          context.write(node, val);
          context.write(dest_int, node_text);
        }
      }
    }
  }

  public static class ResultReducer extends Reducer<IntWritable,Text, IntWritable,Text> {
    private  Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String edges = new String();
      double vect = 0.0;
      for(Text i : values){
        if(i.toString().charAt(0) =='_'){
          vect += Double.parseDouble(i.toString().substring(1));
        }
        else {
          edges += i + " ";
        }
      }
      result.set(edges + vect);
      context.write(key, result);
    }
  }

  public static class LastMapper extends Mapper<Object, Text, IntWritable,Text>{
    private final static IntWritable node = new IntWritable();
    private final static Text vect = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(" |\t");
      vect.set(line[0] + "_" + line[line.length-2]);
      node.set(1);
      context.write(node,vect);
    }
  }

  public static class LastReducer extends Reducer<IntWritable,Text, IntWritable,Text> {
    private static final  Text result = new Text();
    private static final  IntWritable node = new IntWritable();


    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      double max = 0.0;
      int id;
      int max_id = 0;
      double vect;
      for(Text i : values){
        if(i.toString().split("_").length != 2) continue;
        id = Integer.parseInt(i.toString().split("_")[0]);
        vect = Double.parseDouble(i.toString().split("_")[1]);

        if(vect > max){
          max = vect;
          max_id = id;
        }
        result.set(Double.toString(max));
        node.set(max_id);
      }
      context.write(node, result);
    }
  }


  public static void main(String[] args) throws Exception {
/*
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "eigenvector");
    job.setJarByClass(EigenVectorCentrality.class);
    MultipleInputs.addInputPath(job, new Path("input/edgelist.txt"), TextInputFormat.class, EdgeMapper.class);
    MultipleInputs.addInputPath(job, new Path("input/vector.txt"), TextInputFormat.class, VectMapper.class);
    job.setReducerClass(MultReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job, new Path("output/output1"));
    job.waitForCompletion(true);

    String input = "output/output1" ;
    String output = "output/output11";
    int converge = 3;
    while (converge > 0) {
      Configuration conf2 = new Configuration();
      Job job2 = Job.getInstance(conf2, "eigenvector");
      job2.setJarByClass(EigenVectorCentrality.class);
      job2.setMapperClass(ResultMapper.class);
      job2.setCombinerClass(ResultReducer.class);
      job2.setReducerClass(ResultReducer.class);
      job2.setOutputKeyClass(IntWritable.class);
      job2.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job2, new Path(input));
      FileOutputFormat.setOutputPath(job2, new Path(output));
      job2.waitForCompletion(false);

      input+="1";
      output+="1";
      converge--;
    }
 */
    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "eigenvector");
    job3.setJarByClass(EigenVectorCentrality.class);
    job3.setMapperClass(LastMapper.class);
    job3.setCombinerClass(LastReducer.class);
    job3.setReducerClass(LastReducer.class);
    job3.setOutputKeyClass(IntWritable.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path("output/output1111"));
    FileOutputFormat.setOutputPath(job3, new Path("final_output"));
    job3.waitForCompletion(false);

  }
}