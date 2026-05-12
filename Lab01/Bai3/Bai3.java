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

public class Bai3 {

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

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> userGenderMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                Path path = new Path("/users/users.txt");
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        userGenderMap.put(parts[0].trim(), parts[1].trim()); 
                    }
                }
                br.close();
            } catch (Exception e) {
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown";
            double maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.startsWith("T:")) {
                    movieTitle = strVal.substring(2);
                } else if (strVal.startsWith("R:")) {
                    String[] parts = strVal.split(":");
                    String userId = parts[1];
                    double score = Double.parseDouble(parts[2]);
                    
                    String gender = userGenderMap.getOrDefault(userId, "Unknown");
                    if (gender.equalsIgnoreCase("M")) {
                        maleSum += score;
                        maleCount++;
                    } else if (gender.equalsIgnoreCase("F")) {
                        femaleSum += score;
                        femaleCount++;
                    }
                }
            }
            if (maleCount > 0 || femaleCount > 0) {
                String mAvg = (maleCount > 0) ? String.format("%.2f", maleSum / maleCount) : "0.00";
                String fAvg = (femaleCount > 0) ? String.format("%.2f", femaleSum / femaleCount) : "0.00";
                
                String result = String.format("  Male: %s, Female: %s", mAvg, fAvg);
                context.write(new Text(movieTitle), new Text(result));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Analysis");
        
        job.setJarByClass(Bai3.class);
        job.setReducerClass(GenderReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}