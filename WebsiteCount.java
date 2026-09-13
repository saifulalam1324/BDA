import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebsiteCount {
    public static class WebsiteMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private static final Text OUTPUT_KEY = new Text("WebsiteUsers");
        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("AboutMe,AccountId")) {
                return;
            }
            List<String> fields = parseCSV(line);
            if (fields.size() <= 11) {
                return;
            }
            String websiteUrl = fields.get(11).trim();
            if (!websiteUrl.isEmpty()) {
                context.write(OUTPUT_KEY, ONE);
            }
        }
        private List<String> parseCSV(String line) {
            List<String> fields = new ArrayList<>();
            StringBuilder field = new StringBuilder();
            boolean inQuotes = false;
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '"') {
                    if (inQuotes &&
                        i + 1 < line.length() &&
                        line.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        inQuotes = !inQuotes;
                    }
                } else if (c == ',' && !inQuotes) {
                    fields.add(field.toString());
                    field.setLength(0);
                } else {
                    field.append(c);
                }
            }
            fields.add(field.toString());
            return fields;
        }
    }

    public static class WebsiteReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {
                total += value.get();
            }

            context.write(key, new IntWritable(total));
        }
    }

    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Count Users With Website");

        job.setJarByClass(WebsiteCount.class);

        job.setMapperClass(WebsiteMapper.class);

        job.setReducerClass(WebsiteReducer.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0]));

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1]));

        System.exit(
                job.waitForCompletion(true)
                        ? 0
                        : 1);
    }
}