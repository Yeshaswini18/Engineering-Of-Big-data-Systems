package com.yeshaswini.movieperyear;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main( String[] args )
    {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "Partitioning Pattern");
            job.setJarByClass(Driver.class);
            YearPartitioner.setMinLastAccessDate(job,2016);
            //set mappers and reducers classes
            job.setMapperClass(YearPartitionerMapper.class);
            job.setReducerClass(YearPartitionerReducer.class);
            job.setPartitionerClass(YearPartitioner.class);

            //set inputformat and outputformat
            job.setInputFormatClass(TextInputFormat.class);

            job.setNumReduceTasks(5);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);

            //set the output key and value types
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(LongWritable.class);

            //set the input and output args
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));


            Path outDir = new Path(args[1]);

            FileSystem fs = FileSystem.get(job.getConfiguration());
            if(fs.exists(outDir)){
                fs.delete(outDir, true);
            }

            System.exit(job.waitForCompletion(true)? 0:1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
