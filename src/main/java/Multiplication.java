import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation
			//pass data to reducer

			String[] moviePair_relation = value.toString().trim().split("\t");
			context.write(new Text(moviePair_relation[0]), new Text(moviePair_relation[1]));

		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input: user,movie,rating
			// pass data to reducer
			// output: key = movie, value = userA:rating, userB:rating ...

			String[] user_movie_rating = value.toString().trim().split(",");
			String userId = user_movie_rating[0];
			String movieId = user_movie_rating[1];
			String rating = user_movie_rating[2];
			context.write(new Text(movieId), new Text(userId + ":" + rating));
		}
	}

	public static class EstimateRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input: value = userid \t movie1:rating, movie2:rating..., same as CoOccurrenceMatrixGenerator mapper
			// output: key = movie, value = userA:rating, userB:rating ...

			String[] line = value.toString().trim().split("\t");
			String userId = line[0];
			
			if(line[1].contains(",")) {	//  more than one movies
				String[] movie_rating = line[1].split(",");
				for (String mr : movie_rating) {
					String movieId = mr.split(":")[0];
					String rating = mr.split(":")[1];
					context.write(new Text(movieId), new Text(userId + ":" + rating));
				}
			} else {	// only one movie
				String movieId = line[1].split(":")[0];
				String rating = line[1].split(":")[1];
				context.write(new Text(movieId), new Text(userId + ":" + rating));
			}

		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie, then do the multiplication
			// output key = user:movie, value = relation*rating

//			List<String> movies = new ArrayList<String>();
//			List<Double> relations = new ArrayList<Double>();
//			List<String> users = new ArrayList<String>();
//			List<Double> ratings = new ArrayList<Double>();
			Map<String, Double> movieRelationMap = new HashMap<String, Double>();
			Map<String, Double> userRatingMap = new HashMap<String, Double>();
			for(Text value : values) {
				if(value.toString().contains("=")) {	// movie=relation
					String[] movie_relation = value.toString().trim().split("=");
//					movies.add(movie_relation[0]);
//					relations.add(Double.parseDouble(movie_relation[1]));
					movieRelationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
				} else if(value.toString().contains(":")) {		//user:rating
					String[] user_rating = value.toString().trim().split(":");
//					users.add(user_rating[0]);
//					ratings.add(Double.parseDouble(user_rating[1]));
					userRatingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
				}
			}

//			for(int i = 0; i < users.size(); i++) {
//				for(int j = 0; j < movies.size(); j++) {
//					context.write(new Text(users.get(i) + ":" + movies.get(j)), new DoubleWritable(relations.get(j) * ratings.get(i)));
//				}
//			}
			for(Map.Entry<String, Double> entry : userRatingMap.entrySet()) {
				String user = entry.getKey();
				double rating = entry.getValue();
				for(Map.Entry<String, Double> element : movieRelationMap.entrySet()) {
					String movie = element.getKey();
					double relation = element.getValue();
					context.write(new Text(user + ":" + movie), new DoubleWritable(relation * rating));
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
		ChainMapper.addMapper(job, EstimateRatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);
		job.setMapperClass(EstimateRatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, EstimateRatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[3]));
		
		job.waitForCompletion(true);
	}
}
