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

public class MovieRatescore {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str[] = (value.toString()).split(",");
            if(!str[1].equals("rating")) {
                context.write(new Text(str[1].toString()), new DoubleWritable(Double.parseDouble(str[6].toString())));
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reducer(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            double sum = 0;
            for(DoubleWritable val: values) {
                counter++;
                sum += val.get();
            }

            context.write(new Text(key), new DoubleWritable(((double) sum)/ ((double) counter)));
        }
    }
}
