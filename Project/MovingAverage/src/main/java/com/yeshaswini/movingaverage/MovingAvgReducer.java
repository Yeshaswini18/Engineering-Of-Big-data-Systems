package com.yeshaswini.movingaverage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class MovingAvgReducer extends Reducer<MovieDateWritable, DoubleWritable, Text, DoubleWritable> {
    private MultipleOutputs<Text, DoubleWritable> multipleoutput;
    protected void setup(Context context) throws java.io.IOException ,InterruptedException {
        multipleoutput = new MultipleOutputs<Text, DoubleWritable>(context);
    };

    @Override
    protected void reduce(MovieDateWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String movieID = key.getMovieID().toString();
        IntWritable Date = key.getDate();

        Double sum = 0.0;
        int N = 30;
        for (DoubleWritable val : values) {
            sum = sum + val.get();
        }
        sum = sum / (double) N ;

        String key1 = movieID + "    " + Date.toString();
        multipleoutput.write(new Text(key1),new DoubleWritable(sum), movieID);
    }

    protected void cleanup(Context context) throws java.io.IOException ,InterruptedException {
        multipleoutput.close();
    }
}
