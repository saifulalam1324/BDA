import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Top10ScoreMapReduce {


    public static class CSVInputFormat
            extends FileInputFormat<LongWritable, Text> {

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(
                InputSplit split,
                org.apache.hadoop.mapreduce.TaskAttemptContext context) {

            return new CSVRecordReader();
        }


        protected boolean isSplitable(
                org.apache.hadoop.fs.FileSystem fs,
                Path filename) {

            return false;
        }
    }


    public static class CSVRecordReader
            extends RecordReader<LongWritable, Text> {

        private FSDataInputStream inputStream;

        private LongWritable currentKey = new LongWritable();

        private Text currentValue = new Text();

        private long recordNumber = 0;

        private boolean finished = false;

        private int pushedBack = -1;


        private int readByte() throws IOException {

            if (pushedBack != -1) {

                int temp = pushedBack;

                pushedBack = -1;

                return temp;
            }

            return inputStream.read();
        }


        private void unreadByte(int value) {

            pushedBack = value;
        }


        @Override
        public void initialize(
                InputSplit genericSplit,
                org.apache.hadoop.mapreduce.TaskAttemptContext context)
                throws IOException {

            FileSplit split = (FileSplit) genericSplit;

            Path file = split.getPath();

            Configuration conf = context.getConfiguration();

            FileSystem fs = file.getFileSystem(conf);

            inputStream = fs.open(file);
        }


        @Override
        public boolean nextKeyValue() throws IOException {

            if (finished) {
                return false;
            }

            StringBuilder record = new StringBuilder();

            boolean inQuotes = false;

            boolean started = false;


            while (true) {

                int current = readByte();

                if (current == -1) {

                    finished = true;

                    break;
                }

                char c = (char) current;


                if (c == '"') {

                    record.append(c);

                    started = true;


                    if (inQuotes) {

                        int next = readByte();

                        if (next == '"') {

                            record.append('"');

                        } else {

                            inQuotes = false;

                            if (next != -1) {

                                unreadByte(next);
                            }
                        }

                    } else {

                        inQuotes = true;
                    }


                } else if (c == '\n' && !inQuotes) {

                    if (started) {

                        break;
                    }


                } else if (c == '\r' && !inQuotes) {

                    continue;


                } else {

                    record.append(c);

                    started = true;
                }
            }


            if (record.length() == 0) {

                return false;
            }


            recordNumber++;

            currentKey.set(recordNumber);

            currentValue.set(record.toString());

            return true;
        }


        @Override
        public LongWritable getCurrentKey() {

            return currentKey;
        }


        @Override
        public Text getCurrentValue() {

            return currentValue;
        }


        @Override
        public float getProgress() {

            return finished ? 1.0f : 0.0f;
        }


        @Override
        public void close() throws IOException {

            if (inputStream != null) {

                inputStream.close();
            }
        }
    }


    public static class ScoreMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {


        private final Text outputKey = new Text("Score");

        private final IntWritable outputValue = new IntWritable();

        private boolean headerSkipped = false;


        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context)
                throws IOException, InterruptedException {


            String record = value.toString();


            if (!headerSkipped) {

                headerSkipped = true;

                if (record.startsWith("AcceptedAnswerId")) {

                    return;
                }
            }


            List<String> fields = parseCSV(record);


            if (fields.size() <= 17) {

                return;
            }


            try {

                String scoreText = fields.get(17).trim();


                if (scoreText.isEmpty()) {

                    return;
                }


                int score = Integer.parseInt(scoreText);

                outputValue.set(score);

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

                    if (inQuotes
                            && i + 1 < line.length()
                            && line.charAt(i + 1) == '"') {

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
            extends Reducer<Text, IntWritable, IntWritable, Text> {

        private java.util.TreeSet<Integer> top10;

        @Override
        protected void setup(Context context) {
            top10 = new java.util.TreeSet<>();
        }

        @Override
        protected void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            for (IntWritable value : values) {

                int score = value.get();

                top10.add(score);

                if (top10.size() > 10) {
                    top10.pollFirst();
                }
            }

            Integer[] results = top10.descendingSet().toArray(new Integer[0]);

            for (Integer score : results) {

                context.write(
                        new IntWritable(score),
                        new Text("Top Score"));
            }
        }
    }


    public static void main(String[] args)
            throws Exception {


        if (args.length != 2) {

            System.err.println(
                    "Usage: Top10ScoreMapReduce <input> <output>");

            System.exit(2);
        }


        Configuration conf =
                new Configuration();


        Job job =
                Job.getInstance(
                        conf,
                        "Top 10 Post Scores");


        job.setJarByClass(
                Top10ScoreMapReduce.class);


        job.setInputFormatClass(
                CSVInputFormat.class);


        job.setMapperClass(
                ScoreMapper.class);


        job.setReducerClass(
                Top10Reducer.class);


        job.setMapOutputKeyClass(
                Text.class);


        job.setMapOutputValueClass(
                IntWritable.class);


        job.setOutputKeyClass(
                IntWritable.class);


        job.setOutputValueClass(
                Text.class);


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