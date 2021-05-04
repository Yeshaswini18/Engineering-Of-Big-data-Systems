package com.yeshaswini.numsum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NumSumCombiner extends Reducer<IntWritable, MinMaxKey, IntWritable, MinMaxKey> {
    MinMaxKey minMaxKey = new MinMaxKey();
    @Override
    protected void reduce(IntWritable key, Iterable<MinMaxKey> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        long count = 0;
        minMaxKey.setCount(0);
        minMaxKey.setSum(0);
        minMaxKey.setMaximum(Double.MIN_VALUE);
        minMaxKey.setMinimum(Double.MAX_VALUE);
        for(MinMaxKey value: values) {
            sum += value.getRating();
            count += value.getCount();

            if (minMaxKey.getMinimum() == Double.MAX_VALUE || value.getMinimum() < minMaxKey.getMinimum()){
                minMaxKey.setMinimum(value.getMinimum());
            }

            if(minMaxKey.getMaximum() == Double.MIN_VALUE || value.getMaximum() > minMaxKey.getMaximum()) {
                minMaxKey.setMaximum(value.getMaximum());
            }
        }
        minMaxKey.setSum(sum);
        minMaxKey.setCount(count);
        context.write(key, minMaxKey);
    }
}
