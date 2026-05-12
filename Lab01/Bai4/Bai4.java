import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai4 {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                movieIdKey.set(parts[1].trim());
                ratingValue.set("R:" + parts[0].trim() + ":" + parts[2].trim());
                context.write(movieIdKey, ratingValue);
            }
        }
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text titleValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 2) {
                movieIdKey.set(parts[0].trim());
                titleValue.set("T:" + parts[1].trim());
                context.write(movieIdKey, titleValue);
            }
        }
    }

    public static class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Integer> userAgeMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                Path path = new Path("/users/users.txt");
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 3) {
                        userAgeMap.put(parts[0].trim(), Integer.parseInt(parts[2].trim()));
                    }
                }
                br.close();
            } catch (Exception e) {}
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown";
            double[] sums = new double[4];
            int[] counts = new int[4];

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.startsWith("T:")) {
                    movieTitle = strVal.substring(2);
                } else if (strVal.startsWith("R:")) {
                    String[] parts = strVal.split(":");
                    String userId = parts[1];
                    double score = Double.parseDouble(parts[2]);
                    
                    int age = userAgeMap.getOrDefault(userId, -1);
                    if (age >= 0 && age <= 18) { 
                        sums[0] += score; counts[0]++; 
                    }
                    else if (age > 18 && age <= 35) { 
                        sums[1] += score; counts[1]++; 
                    }
                    else if (age > 35 && age <= 50) { 
                        sums[2] += score; counts[2]++; 
                    }
                    else if (age > 50) { 
                        sums[3] += score; counts[3]++; 
                    }
                }
            }

            if (movieTitle != "Unknown") {
                StringBuilder result = new StringBuilder();
                String[] labels = {"0-18", "18-35", "35-50", "50+"};
                
                for (int i = 0; i < 4; i++) {
                    String avgStr = (counts[i] > 0) ? String.format("%.2f", sums[i] / counts[i]) : "NA";
                    result.append(String.format("%s: %s  ", labels[i], avgStr));
                }
                context.write(new Text(movieTitle), new Text(result.toString().trim()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Analysis");
        job.setJarByClass(Bai4.class);
        job.setReducerClass(AgeGroupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}