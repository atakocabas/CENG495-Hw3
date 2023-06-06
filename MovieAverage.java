import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.spi.DirStateFactory.Result;

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

public class MovieAverage {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String str[] = (value.toString()).split(",");
                if(!str[-1].equals("duration")) {
                    DoubleWritable val = new DoubleWritable(Double.parseDouble(str[-1]));
                    context.write(new Text("duration"), val);
                }
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public double total;
        public int counter = 0;

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            try {
                for (DoubleWritable val : values) {
                    total += val.get();
                    counter++;
                }

                double res = total / ((double) counter);
                context.write(new Text("average duration"), new DoubleWritable(res));
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }
    }
}
