package com.yeshaswini.movingaverage;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1,"MovingAvgPart1");
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(MovingAvgPart1Mapper.class);

        job1.setReducerClass(MovingAvgPart1Reducer.class);

        job1.setSortComparatorClass(MovingAverageComparator.class);
        job1.setGroupingComparatorClass(MovingAvgGrpComparator.class);

        job1.setMapOutputKeyClass(MovieDateWritable.class);
        job1.setMapOutputValueClass(Text.class);


        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        Configuration conf = new Configuration();

        Job job = new Job(conf, "Moving Avg job2");

        job.setJarByClass(Driver.class);
        job.setMapperClass(MovingAvgMapper.class);
        job.setReducerClass(MovingAvgReducer.class);

        job.setSortComparatorClass(MovingAverageComparator.class);

        job.setMapOutputKeyClass(MovieDateWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        if (job1.waitForCompletion(true)) {
            System.exit(job.waitForCompletion(true)? 0:2);
        }
    }
}
