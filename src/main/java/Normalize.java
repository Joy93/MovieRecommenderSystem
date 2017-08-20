import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //movieA:movieB \t relation
            //collect the relationship list for movieA
            //output: <mB, mA:relation>
            String line = value.toString().trim();
            String[] inputs = line.split("\t");
            String[] ids = inputs[0].split(":");
            //ids[0] key;
            String val = ids[1] + ":" + inputs[1];
            context.write(new Text(ids[0]), new Text(val));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = movieA, value=<movieB:relation, movieC:relation...>
            //normalize each unit of co-occurrence matrix
            //Output: <mA, mB: normalized relation>

            HashMap<String, Integer> rawMap = new HashMap<String, Integer>();
            int denominator = 0;
            for(Text value: values) {
                String[] inputs = value.toString().trim().split(":");
                int relation = Integer.parseInt(inputs[1]);
                rawMap.put(inputs[0], relation);
                denominator += relation;
            }
            Iterator iterator = rawMap.keySet().iterator();
            while(iterator.hasNext()) {
                String movie_id = iterator.next().toString();
                context.write(new Text(movie_id), new Text(key + ":" + (double)rawMap.get(movie_id) / denominator));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
