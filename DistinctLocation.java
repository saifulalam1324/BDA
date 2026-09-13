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

public class DistinctLocation {

    public static class LocationMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);

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

            if (fields.size() <= 7) {
                return;
            }

            String location = fields.get(7).trim();

            if (!location.isEmpty()) {
                context.write(new Text(location), ONE);
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

    public static class LocationReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            int count = 0;

            for (IntWritable value : values) {
                count += value.get();
            }

            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Distinct User Locations");

        job.setJarByClass(DistinctLocation.class);

        job.setMapperClass(LocationMapper.class);

        job.setReducerClass(LocationReducer.class);

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