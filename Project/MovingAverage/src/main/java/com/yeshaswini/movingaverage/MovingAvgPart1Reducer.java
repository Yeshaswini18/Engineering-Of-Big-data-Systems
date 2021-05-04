package com.yeshaswini.movingaverage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovingAvgPart1Reducer extends Reducer<MovieDateWritable, Text, Text, Text> {
    @Override
    protected void reduce(MovieDateWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String movieID = key.getMovieID().toString();
        String date = key.getDate().toString();

        for (Text val : values) {

            context.write(new Text(movieID), val);
        }
    }
}
