import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class  Bai1 {
    
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length >=3) {
                movieIdKey.set(parts[1].trim()); 
                ratingValue.set("RATING:" + parts[2].trim());
            
                context.write(movieIdKey, ratingValue);
            }     
        }
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text movieTitleValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            } 

            String[] parts = line.split(",");
            if (parts.length >= 2) {
                movieIdKey.set(parts[0].trim());
                movieTitleValue.set("TITLE:" + parts[1].trim());
                context.write(movieIdKey, movieTitleValue);
            }
        }
    }

    public static class RatingReduce extends Reducer<Text, Text, Text, Text> {
        private String maxMovie = "";
        private double maxRating = -1.0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String title = "Unknown";
            double sum = 0;
            int count = 0;

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.startsWith("TITLE:")) {
                    title = strVal.substring(6);
                } else if (strVal.startsWith("RATING:")) {
                    sum += Double.parseDouble(strVal.substring(7));
                    count++;
                }
            }

            if (count > 0) {
                double avg = sum / count;
                String outputStr = "Average rating: " + avg + " (Total ratings: " + count + ")";
                context.write(new Text(title), new Text(outputStr));

                if (count >= 5 && avg > maxRating) {
                    maxRating = avg;
                    maxMovie = title;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                context.write(new Text("\n" + maxMovie), new Text("is the highest rated movie with an average rating of " 
                    + maxRating + " among movies with at least 5 ratings."));
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Movie Rating Analysis");

        job.setJarByClass(RatingReduce.class);

        job.setMapperClass(RatingMapper.class);
        job.setMapperClass(MovieMapper.class);

        job.setReducerClass(RatingReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(
            job, 
            new Path(args[0]), 
            TextInputFormat.class, 
            RatingMapper.class
        );
        MultipleInputs.addInputPath(
            job, 
            new Path(args[1]), 
            TextInputFormat.class, 
            MovieMapper.class
        );

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}