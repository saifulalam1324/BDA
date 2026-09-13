import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

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

public class Top10Reputation {

    public static class ReputationMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable();

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

            if (fields.size() <= 8) {
                return;
            }

            String userId = fields.get(5).trim();
            String reputationText = fields.get(8).trim();

            if (userId.isEmpty() || reputationText.isEmpty()) {
                return;
            }

            try {
                int reputation = Integer.parseInt(reputationText);

                outputKey.set(userId);
                outputValue.set(reputation);

                context.write(outputKey, outputValue);

            } catch (NumberFormatException e) {
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

    public static class Top10Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private PriorityQueue<UserReputation> top10;

        @Override
        protected void setup(Context context) {

            top10 = new PriorityQueue<>(10);
        }

        @Override
        protected void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            for (IntWritable value : values) {

                int reputation = value.get();

                UserReputation user =
                        new UserReputation(
                                key.toString(),
                                reputation);

                if (top10.size() < 10) {

                    top10.add(user);

                } else if (reputation > top10.peek().reputation) {

                    top10.poll();

                    top10.add(user);
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            List<UserReputation> results =
                    new ArrayList<>(top10);

            Collections.sort(
                    results,
                    (a, b) ->
                            Integer.compare(
                                    b.reputation,
                                    a.reputation));

            for (UserReputation user : results) {

                context.write(
                        new Text(user.userId),
                        new IntWritable(user.reputation));
            }
        }
    }

    public static class UserReputation
            implements Comparable<UserReputation> {

        String userId;
        int reputation;

        UserReputation(
                String userId,
                int reputation) {

            this.userId = userId;
            this.reputation = reputation;
        }

        @Override
        public int compareTo(UserReputation other) {

            return Integer.compare(
                    this.reputation,
                    other.reputation);
        }
    }

    public static void main(String[] args)
            throws Exception {

        Configuration conf =
                new Configuration();

        Job job =
                Job.getInstance(
                        conf,
                        "Top 10 User Reputation");

        job.setJarByClass(
                Top10Reputation.class);

        job.setMapperClass(
                ReputationMapper.class);

        job.setReducerClass(
                Top10Reducer.class);

        job.setMapOutputKeyClass(
                Text.class);

        job.setMapOutputValueClass(
                IntWritable.class);

        job.setOutputKeyClass(
                Text.class);

        job.setOutputValueClass(
                IntWritable.class);

        job.setNumReduceTasks(1);

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