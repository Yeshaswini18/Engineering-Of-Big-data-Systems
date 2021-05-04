package com.yeshaswini.contentbasedrecom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Path out = new Path(args[1]);

        Job job1 = Job.getInstance(conf, "Contentbased output1");
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(MapperPart1.class);
        job1.setReducerClass(ReducerPart1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Contentbased output2");
        job2.setJarByClass(Driver.class);
        job2.setMapperClass(MapperPart2.class);
        job2.setReducerClass(ReducerPart2.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(out, "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job3 = Job.getInstance(conf, "Contentbased output3");
        job3.setJarByClass(Driver.class);
        job3.setMapperClass(MapperPart3.class);
        job3.setReducerClass(ReducerPart3.class);
        job3.setNumReduceTasks(1);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job3, new Path(out, "out2"));
        FileOutputFormat.setOutputPath(job3, new Path(out, "out3"));
        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}
