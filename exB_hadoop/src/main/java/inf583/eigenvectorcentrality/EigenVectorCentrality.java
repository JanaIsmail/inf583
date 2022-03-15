package inf583.eigenvectorcentrality;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class EigenVectorCentrality {

  public static class TokenizerMapper
          extends Mapper<Object, Text, DoubleWritable, DoubleWritable>{

    private final static DoubleWritable node = new DoubleWritable();
    private final static DoubleWritable edge = new DoubleWritable();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      String[] line = value.toString().split(" ");
      node.set(Double.parseDouble(line[0]));
      for(int i =1 ;  i<line.length ;  i++ ){
        edge.set(Double.parseDouble(line[i]));
        context.write(node,edge);
      }
    }
  }

  public static class IntSumReducer
          extends Reducer<DoubleWritable,DoubleWritable, DoubleWritable,DoubleWritable> {
    private final static DoubleWritable result = new DoubleWritable();

    public void reduce(DoubleWritable key, Iterable<DoubleWritable> values,
                       Context context
    ) throws IOException, InterruptedException {

      ArrayList<Double> vector = new ArrayList<>();
      // FAIRE UNE BOUCLE
      for(int i =0; i<64375; i++){
        vector.add( 1.0/64375);
      }

      double sum = 0.0;
      int i = 0;
      for (DoubleWritable val : values) {
        sum += vector.get((int)val.get());
        i++;
      }

      System.out.println(i);
      result.set(sum);
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
    job.setOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path("input"));
    FileOutputFormat.setOutputPath(job, new Path("output"));
    job.waitForCompletion(true);

  }
}