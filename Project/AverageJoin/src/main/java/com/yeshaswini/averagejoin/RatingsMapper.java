package com.yeshaswini.averagejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] values = data.split(",",-1);

        if (values.length == 4 && !values[1].equals("movieId") && !values[2].equals("rating")) {
            context.write(new Text(values[1]), new Text("R"+values[2]));
        }
    }
}
