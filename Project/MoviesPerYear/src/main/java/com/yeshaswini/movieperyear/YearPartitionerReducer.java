package com.yeshaswini.movieperyear;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class YearPartitionerReducer extends Reducer<IntWritable, IntWritable, IntWritable, LongWritable> {
    LongWritable countOut = new LongWritable();
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (IntWritable i: values){
            count = count + 1;
        }
        countOut.set(count);
        context.write(key, countOut);
    }
}
