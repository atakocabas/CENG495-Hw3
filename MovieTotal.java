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

public class MovieTotal {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            try {
                String str[] = (value.toString()).split(",");
                String name = str[0];
                String time = str[-1];
                if(!time.equals("duration")) {
                    context.write(new Text(name), new IntWritable(Integer.parseInt(time.toString())));
                }
            } catch (Exception exception) {
                System.out.println(exception.getMessage());
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        public int total = 0;
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for(IntWritable val: values){
                total += val.get();
            }
            context.write(new Text("total"), new IntWritable(total));
        }
    }
}
