import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PostTypeCount {

    // ============================================================
    // CSV RECORD READER
    // ============================================================

    public static class CSVRecordReader
            extends RecordReader<LongWritable, Text> {

        private DataInputStream inputStream;

        private long start;
        private long end;
        private long position;

        private LongWritable currentKey =
                new LongWritable();

        private Text currentValue =
                new Text();

        private boolean firstSplitRecord = true;

        @Override
        public void initialize(
                InputSplit genericSplit,
                TaskAttemptContext context)
                throws IOException {

            FileSplit split =
                    (FileSplit) genericSplit;

            start = split.getStart();
            end = start + split.getLength();

            Path file =
                    split.getPath();

            InputStream fileInput =
                    file.getFileSystem(
                                    context.getConfiguration())
                            .open(file);

            inputStream =
                    new DataInputStream(fileInput);

            inputStream.skip(start);

            position = start;

            /*
             * If this is not the first split,
             * move to the beginning of the next
             * complete CSV record.
             */
            if (start != 0) {
                skipFirstPartialRecord();
            }
        }

        private void skipFirstPartialRecord()
                throws IOException {

            boolean insideQuotes = false;

            int character;

            while ((character = inputStream.read()) != -1) {

                position++;

                if (character == '"') {
                    insideQuotes = !insideQuotes;
                }

                if (character == '\n' &&
                        !insideQuotes) {

                    break;
                }
            }
        }

        @Override
        public boolean nextKeyValue()
                throws IOException {

            /*
             * After the split end, do not start
             * another record. However, if a record
             * has already started before the split
             * end, continue reading until the record
             * is complete.
             */
            if (position >= end &&
                    !firstSplitRecord) {

                return false;
            }

            StringBuilder record =
                    new StringBuilder();

            boolean insideQuotes = false;
            boolean started = false;

            int character;

            while ((character =
                    inputStream.read()) != -1) {

                position++;

                if (character == '"') {

                    insideQuotes = !insideQuotes;

                    record.append('"');

                    started = true;

                    continue;
                }

                /*
                 * Newline outside quotes means
                 * the CSV record has ended.
                 */
                if (character == '\n' &&
                        !insideQuotes) {

                    break;
                }

                /*
                 * Ignore carriage return.
                 */
                if (character == '\r') {
                    continue;
                }

                record.append(
                        (char) character);

                started = true;
            }

            if (!started &&
                    record.length() == 0) {

                return false;
            }

            currentKey.set(position);

            currentValue.set(
                    record.toString());

            firstSplitRecord = false;

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

            if (end <= start) {
                return 1.0f;
            }

            float progress =
                    (float) (position - start)
                            / (float) (end - start);

            if (progress < 0.0f) {
                progress = 0.0f;
            }

            if (progress > 1.0f) {
                progress = 1.0f;
            }

            return progress;
        }

        @Override
        public void close()
                throws IOException {

            if (inputStream != null) {
                inputStream.close();
            }
        }
    }


    // ============================================================
    // CUSTOM CSV INPUT FORMAT
    // ============================================================

    public static class CSVInputFormat
            extends FileInputFormat<LongWritable, Text> {

        @Override
        public RecordReader<LongWritable, Text>
        createRecordReader(
                InputSplit split,
                TaskAttemptContext context) {

            return new CSVRecordReader();
        }

        protected boolean isSplitable(
                org.apache.hadoop.fs.FileSystem fs,
                Path filename) {

            return true;
        }
    }


    // ============================================================
    // CSV PARSER
    // ============================================================

    public static String[] parseCSV(
            String line) {

        List<String> fields =
                new ArrayList<>();

        StringBuilder currentField =
                new StringBuilder();

        boolean insideQuotes = false;

        for (int i = 0;
             i < line.length();
             i++) {

            char character =
                    line.charAt(i);

            /*
             * Handle quotation marks.
             */
            if (character == '"') {

                /*
                 * Two quotes inside a quoted field
                 * represent one quote.
                 */
                if (insideQuotes &&
                        i + 1 < line.length() &&
                        line.charAt(i + 1) == '"') {

                    currentField.append('"');

                    i++;

                } else {

                    insideQuotes =
                            !insideQuotes;
                }

            }

            /*
             * Comma outside quotes separates fields.
             */
            else if (character == ',' &&
                    !insideQuotes) {

                fields.add(
                        currentField.toString());

                currentField.setLength(0);
            }

            else {

                currentField.append(
                        character);
            }
        }

        /*
         * Add the final field.
         */
        fields.add(
                currentField.toString());

        return fields.toArray(
                new String[0]);
    }


    // ============================================================
    // MAPPER
    // ============================================================

    public static class PostTypeMapper
            extends Mapper<LongWritable, Text,
            Text, IntWritable> {

        private static final IntWritable ONE =
                new IntWritable(1);

        private final Text outputKey =
                new Text();

        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context)
                throws IOException,
                InterruptedException {

            String line =
                    value.toString();

            /*
             * Ignore empty records.
             */
            if (line.trim().isEmpty()) {
                return;
            }

            String[] fields =
                    parseCSV(line);

            /*
             * PostTypeId is index 17.
             *
             * Therefore we need at least
             * 18 fields.
             */
            if (fields.length <= 17) {
                return;
            }

            /*
             * Skip header.
             */
            if (fields[0]
                    .trim()
                    .equalsIgnoreCase(
                            "AcceptedAnswerId")) {

                return;
            }

            /*
             * PostTypeId = field 17.
             */
            String postTypeId =
                    fields[17].trim();

            /*
             * PostTypeId 1 = Question
             */
            if (postTypeId.equals("1")) {

                outputKey.set(
                        "Questions");

                context.write(
                        outputKey,
                        ONE);
            }

            /*
             * PostTypeId 2 = Answer
             */
            else if (postTypeId.equals("2")) {

                outputKey.set(
                        "Answers");

                context.write(
                        outputKey,
                        ONE);
            }
        }
    }


    // ============================================================
    // REDUCER
    // ============================================================

    public static class PostTypeReducer
            extends Reducer<Text, IntWritable,
            Text, IntWritable> {

        private final IntWritable result =
                new IntWritable();

        @Override
        protected void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException,
                InterruptedException {

            int total = 0;

            for (IntWritable value : values) {

                total += value.get();
            }

            result.set(total);

            context.write(
                    key,
                    result);
        }
    }


    // ============================================================
    // DRIVER
    // ============================================================

    public static void main(
            String[] args)
            throws Exception {

        if (args.length != 2) {

            System.err.println(
                    "Usage: PostTypeCount <input> <output>");

            System.exit(2);
        }

        Configuration configuration =
                new Configuration();

        Job job =
                Job.getInstance(
                        configuration,
                        "Post Type Count");

        job.setJarByClass(
                PostTypeCount.class);

        /*
         * Use custom CSV InputFormat.
         */
        job.setInputFormatClass(
                CSVInputFormat.class);

        /*
         * Input path.
         */
        FileInputFormat.addInputPath(
                job,
                new Path(args[0]));

        /*
         * Mapper.
         */
        job.setMapperClass(
                PostTypeMapper.class);

        /*
         * Combiner.
         *
         * The reducer can safely be used
         * as a combiner because addition
         * is associative and commutative.
         */
        job.setCombinerClass(
                PostTypeReducer.class);

        /*
         * Reducer.
         */
        job.setReducerClass(
                PostTypeReducer.class);

        /*
         * Output types.
         */
        job.setOutputKeyClass(
                Text.class);

        job.setOutputValueClass(
                IntWritable.class);

        /*
         * Output path.
         */
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1]));

        /*
         * Start the job.
         */
        System.exit(
                job.waitForCompletion(true)
                        ? 0
                        : 1);
    }
}