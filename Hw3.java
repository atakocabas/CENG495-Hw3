import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Hw3 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "main");

        job.setJarByClass(Hw3.class);

        if (args[0].equals("total")) {
            job.setMapperClass(MovieTotal.TokenizerMapper.class);
            job.setCombinerClass(MovieTotal.IntSumReducer.class);
            job.setReducerClass(MovieTotal.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
        }

        else if (args[0].equals("average")) {
        }

        else if (args[0].equals("employment")) {
        }

        else if (args[0].equals("ratescore")) {
        }
        else if(args[0].equals("genrescore")){
            
        }

        // output jar file will be tested as: hadoop jar Hw3.jar Hw3 cap input output_c
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}
