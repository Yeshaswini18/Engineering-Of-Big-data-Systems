package com.yeshaswini.numsum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NumSumReducer extends Reducer<IntWritable, MinMaxKey, IntWritable, Summarization> {
    Summarization summarization = new Summarization();

    @Override
    protected void reduce(IntWritable key, Iterable<MinMaxKey> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        long count = 0;
        summarization.setCount(0);
        summarization.setMaximum(Double.MIN_VALUE);
        summarization.setMinimum(Double.MAX_VALUE);
        List<Double>  sortedArray = new ArrayList<>();
        double summation = 0.0;

        for(MinMaxKey minMaxKey: values) {
            sum += minMaxKey.getSum();
            count += minMaxKey.getCount();
            // find minimum and maximum
            if (summarization.getMinimum() == Double.MAX_VALUE || minMaxKey.getMinimum() < summarization.getMinimum()){
                summarization.setMinimum(minMaxKey.getMinimum());
            }

            if(summarization.getMaximum() == Double.MIN_VALUE || minMaxKey.getMaximum() > summarization.getMaximum()) {
                summarization.setMaximum(minMaxKey.getMaximum());
            }

            sortedArray.add(minMaxKey.getRating());
        }

        // Median
        Collections.sort(sortedArray);
        int length = sortedArray.size();
        if (length%2 == 0) {
            summarization.setMedian((sortedArray.get((length-1)/2) + sortedArray.get(length/2))/2);
        }else {
            summarization.setMedian(sortedArray.get(length/2));
        }

        // Average
        double average = sum/length;
        summarization.setAverage(average);
        summarization.setCount(length);

        // Standard Deviation
        for(Double rating : sortedArray) {
            summation += (rating - average) * (rating - average);
        }
        summarization.setStandardDeviation(Math.sqrt(summation/length));
        context.write(key, summarization);
    }
}
