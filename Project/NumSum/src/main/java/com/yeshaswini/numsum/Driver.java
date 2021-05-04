package com.yeshaswini.numsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args){
        try {
            Configuration config = new Configuration();
            Job job = Job.getInstance(config, "Numerical Summarization");

            job.setJarByClass(Driver.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(MinMaxKey.class);

            job.setMapperClass(NumSumMapper.class);
            job.setCombinerClass(NumSumCombiner.class);
            job.setReducerClass(NumSumReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Summarization.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            Path outDir = new Path(args[1]);
            FileOutputFormat.setOutputPath(job, outDir);

            job.setNumReduceTasks(1);

            FileSystem fs = FileSystem.get(job.getConfiguration());
            if(fs.exists(outDir)){
                fs.delete(outDir, true);
            }

            System.exit(job.waitForCompletion(true )? 1: 0);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
