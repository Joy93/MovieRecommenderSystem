import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.swing.text.html.HTMLDocument;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA : relation

			//pass data to reducer
			String line = value.toString().trim();
			String[] inputs = line.split("\t");
			context.write(new Text(inputs[0]), new Text(inputs[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: user,movie,rating
			//pass data to reducer
			String line = value.toString().trim();
			String[] inputs = line.split(",");
			//key inputs[1]
			String val = inputs[0] + "=" + inputs[2];
			context.write(new Text(inputs[1]), new Text(val));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB value = <movieA : relation, movieC : relation... userA=rating, userB=rating...>
			//collect the data for each movie, then do the multiplication
			HashMap<String, Double> occurrence = new HashMap<String, Double>();
			HashMap<String, Double> rating = new HashMap<String, Double>();
			double sum_rating = 0;
			int count = 0;
			for(Text value: values) {
				String val = value.toString();
				if(val.contains(":")) {
					String[] movie_relation = val.split(":");
					occurrence.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
				}else {
					String[] user_rating = val.split("=");
					double rate = Double.parseDouble(user_rating[1]);
					if(rate == 0) {
						count++;
					}
					sum_rating += rate;
					rating.put(user_rating[0], rate);
				}
			}
			double avgRate = sum_rating / count;
			Iterator iterator_user = rating.keySet().iterator();
			Iterator iterator_movie = occurrence.keySet().iterator();

			while(iterator_user.hasNext()) {
				String userId = iterator_user.next().toString();
				while(iterator_movie.hasNext()) {
					String movieId = iterator_movie.next().toString();
					String k = userId + ':' + movieId;
					double r = rating.get(userId);
					if(r == 0) {
						r = avgRate;
					}
					context.write(new Text(k), new DoubleWritable(occurrence.get(movieId) * r));
				}
			}

		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
