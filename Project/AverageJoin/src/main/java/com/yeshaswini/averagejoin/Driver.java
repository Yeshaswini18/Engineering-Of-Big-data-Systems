package com.yeshaswini.averagejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args){
        try {
            Configuration config = new Configuration();
            Job job = Job.getInstance(config, "Secondary Sort");

            job.setJarByClass(Driver.class);

            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MetadataMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingsMapper.class);

            job.getConfiguration().set("join.type","inner");
            job.setReducerClass(JoinReducer.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, new Path(args[2]));

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            System.exit(job.waitForCompletion(true)? 0:2);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
