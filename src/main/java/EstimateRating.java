import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EstimateRating {

    public static class EstimateRatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: user,movie,rating
            // divide data by user and make a movie list
            // output: key = user, value = movie:rating

            String[] user_movie_rating = value.toString().trim().split(",");
            int userId = Integer.parseInt(user_movie_rating[0]);
            String movieId = user_movie_rating[1];
            String rating = user_movie_rating[2];
            context.write(new IntWritable(userId), new Text(movieId + ":" + rating));

        }
    }

    public static class EstimateRatingReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        private Set<String> movieList = new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException{

            // rawdata: user,movie,rating

            Configuration configuration = context.getConfiguration();
            String raw = configuration.get("rawdata", "");
            Path pt = new Path(raw);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line = br.readLine();
            while (line!=null) {
                String movieId = line.toString().trim().split(",")[1];
                if (!movieList.contains(movieId)) {
                    movieList.add(movieId);
                }
                line = br.readLine();
            }

        }

        // reduce method
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //merge data for one user
            //input: key = user, value = movie:rating
            //output: key = user, value = unratedMovie1:averageRating, unratedMovie2:averageRating...

            double averageRating = 0.0;
            int count = 0;
            Set<String> rated = new HashSet<String>();
            for (Text value : values) {
                String[] movie_rating = value.toString().trim().split(":");
                rated.add(movie_rating[0]);
                averageRating += Double.parseDouble(movie_rating[1]);
                count++;
            }
            if(count > 0) {
                averageRating /= count;
            }

            StringBuilder movie_estimateRating = new StringBuilder();
            for(String movie : movieList) {
                if(!rated.contains(movie)) {
                    movie_estimateRating.append(movie + ":" + averageRating + ",");
                }
            }
            if(movie_estimateRating.length() > 0) {
                movie_estimateRating.deleteCharAt(movie_estimateRating.length() - 1);
            }
            context.write(key, new Text(movie_estimateRating.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("rawdata", args[0]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(EstimateRating.class);

        job.setMapperClass(EstimateRatingMapper.class);
        job.setReducerClass(EstimateRatingReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[1]));
        //FileInputFormat.addInputPath(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        //FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}
