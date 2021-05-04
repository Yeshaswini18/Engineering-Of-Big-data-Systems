package com.yeshaswini.numsum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NumSumMapper extends Mapper<LongWritable, Text, IntWritable, MinMaxKey> {
    MinMaxKey minMaxKey = new MinMaxKey();
    IntWritable movieID = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",", -1);
        double ratings = 0.0;

        if (tokens.length == 4 && tokens[2].length() != 0 && !tokens[2].equals("rating")) {
            ratings = Double.parseDouble(tokens[2]);
            minMaxKey.setMinimum(ratings);
            minMaxKey.setMaximum(ratings);
            minMaxKey.setCount(1);
            minMaxKey.setRating(ratings);
            movieID.set(Integer.parseInt(tokens[1]));
            context.write(movieID, minMaxKey);
        }

    }
}
