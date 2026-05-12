import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2 {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(",");
            if (parts.length >= 3) {
                movieIdKey.set(parts[1].trim());
                ratingValue.set("RATING:" + parts[2].trim());
                context.write(movieIdKey, ratingValue);
            }
        }
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text genresValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(",");
            if (parts.length >= 3) {
                movieIdKey.set(parts[0].trim());
                genresValue.set("GENRES:" + parts[2].trim());
                context.write(movieIdKey, genresValue);
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Double> genreSumRating = new HashMap<>();
        private Map<String, Integer> genreCount = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> genresOfMovie = new ArrayList<>();
            List<Double> ratingsOfMovie = new ArrayList<>();

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.startsWith("GENRES:")) {
                    String[] genres = strVal.substring(7).split("\\|");
                    for (String g : genres) genresOfMovie.add(g.trim());
                } else if (strVal.startsWith("RATING:")) {
                    ratingsOfMovie.add(Double.parseDouble(strVal.substring(7)));
                }
            }

            for (String genre : genresOfMovie) {
                for (Double r : ratingsOfMovie) {
                    genreSumRating.put(genre, genreSumRating.getOrDefault(genre, 0.0) + r);
                    genreCount.put(genre, genreCount.getOrDefault(genre, 0) + 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<String> sortedGenres = new ArrayList<>(genreSumRating.keySet());
            Collections.sort(sortedGenres);

            for (String genre : sortedGenres) {
                double avg = genreSumRating.get(genre) / genreCount.get(genre);
                String result = String.format("Avg: %.2f, Count: %d", avg, genreCount.get(genre));
                context.write(new Text(genre), new Text(result));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analysis");
        job.setJarByClass(Bai2.class);
        job.setReducerClass(GenreReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}