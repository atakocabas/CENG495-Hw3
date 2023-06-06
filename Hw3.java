import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
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
            job.setReducerClass(MovieTotal.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
        }

        else if (args[0].equals("average")) {
            job.setMapperClass(MovieAverage.TokenizerMapper.class);
            job.setReducerClass(MovieAverage.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        }

        else if (args[0].equals("employment")) {
            job.setMapperClass(MovieEmployment.TokenizerMapper.class);
            job.setReducerClass(MovieEmployment.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
        }

        else if (args[0].equals("ratescore")) {
            job.setMapperClass(MovieRatescore.TokenizerMapper.class);
            job.setReducerClass(MovieRatescore.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        }
        else if(args[0].equals("genrescore")){
            
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}
