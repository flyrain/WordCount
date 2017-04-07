package com.yufeigu;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WordCount {
  static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {
    LOG.info("Word count starting ...");
    JobConf jobConf = new JobConf();
    String[] files = new GenericOptionsParser(jobConf, args).getRemainingArgs();
    Path input = new Path(files[0]);
    Path output = new Path(files[1]);

    jobConf.setJobName("wordcount");
    Job job = Job.getInstance(jobConf);
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordCount.MapForWordCount.class);
    job.setReducerClass(WordCount.ReduceForWordCount.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context con)
        throws IOException, InterruptedException {
      String line = value.toString();
      String[] words = line.split(",");
      for (String word : words) {
        Text outputKey = new Text(word.toUpperCase().trim());
        IntWritable outputValue = new IntWritable(1);
        con.write(outputKey, outputValue);
      }
    }
  }

  public static class ReduceForWordCount
      extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text word, Iterable<IntWritable> values, Context con)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      con.write(word, new IntWritable(sum));
    }
  }
}
