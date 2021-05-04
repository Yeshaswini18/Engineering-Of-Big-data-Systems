package com.yeshaswini.movieperyear;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class YearPartitionerMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    IntWritable yearOut = new IntWritable();
    IntWritable countOut = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] fields = data.split("\\|", -1);

        if (fields.length != 24 || fields[14].equals("release_date") || fields[14].length() != 10 || fields[8].length() == 0) {
            return;
        }

        int year = Integer.parseInt(fields[14].substring(0,4));
        if(year >= 2016) {
            yearOut.set(year);
            countOut.set(1);
            context.write(yearOut, countOut);
        }

    }
}
