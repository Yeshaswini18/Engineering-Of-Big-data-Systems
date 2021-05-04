package com.yeshaswini.womendirector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "WomenDirector");
        job.setJarByClass(Driver.class);
        configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MetadataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), KeyValueTextInputFormat.class, KeywordMapper.class);



        job.getConfiguration().set("join.type","inner");
        job.setReducerClass(WomenDirectorReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true)? 0:2);
    }
}
