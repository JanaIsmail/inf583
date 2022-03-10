package inf583.eigenvectorcentrality;


import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

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

  public static ArrayList<Double> vector = new ArrayList<>();

  public EigenVectorCentrality(){

    for(int i =0; i<64375; i++){
      vector.set(i, 1.0/64375);
    }
  }

 
  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{

    private final static IntWritable node = new IntWritable();
    private final static IntWritable edge = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String line[] = itr.nextToken().split(" ");
        node.set(Integer.parseInt(line[0]));
        for(int i =1 ;  i<line.length ;  i++ ){
          edge.set(Integer.parseInt(line[i]));
          context.write(node,edge);
        }
      }

    }
  }

  public static class IntSumReducer
       extends Reducer<IntWritable,IntWritable, IntWritable,DoubleWritable> {
    private final static DoubleWritable result = new DoubleWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0.0;
      for (IntWritable val : values) {
        sum += vector.get(key.get());
      }
      result.set(sum);
      vector.set(key.get(),sum);
      context.write(key, result); 

    }
  }

  public static void main(String[] args) throws Exception {

    // FAIRE UNE BOUCLE
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "eigenvector");
    job.setJarByClass(EigenVectorCentrality.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path("input"));
    FileOutputFormat.setOutputPath(job, new Path("output"));
    job.waitForCompletion(true);

  }
}