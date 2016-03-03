package com.becausewecangeek.wordcount.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

/**
 * A MapReduce implementation for a simple Word Count. Based on the Hadoop example code as found on:
 * <a href="https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0">https://hadoop.apache.org/</a>
 *
 * @author www.becausewecangeek.com
 */
public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new WordCount(), args);
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Check if the user provided a job name
        String jobName = (args.length >= 3 ? args[2] : "BWCGWordCount");
        job.setJobName(jobName);

        // Run the job
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Mapper
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static Pattern specialCharsRemovePattern = Pattern.compile("[^a-zA-Z]");
        private final static IntWritable one = new IntWritable(1);

        private InputStream modelInputStream;
        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            modelInputStream = getClass().getClassLoader().getResourceAsStream("en-token.bin");
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = specialCharsRemovePattern.matcher(value.toString()).replaceAll(" ").toLowerCase().split("\\s+");
            for(String token : tokens) {
                word.set(token);
                context.write(word, one);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            if(modelInputStream != null) {
                modelInputStream.close();
            }
        }
    }

    /**
     * Reducer
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
