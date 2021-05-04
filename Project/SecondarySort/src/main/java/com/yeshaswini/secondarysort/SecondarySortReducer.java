package com.yeshaswini.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondarySortReducer extends Reducer<CompositeKeyClass, IntWritable, Text, IntWritable> {
    Text ans = new Text();
    IntWritable count = new IntWritable();
    @Override
    protected void reduce(CompositeKeyClass key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for(IntWritable i : values) {
            sum += i.get();
        }
        ans.set(key.getProductionCompany() + " , " + key.getYear());
        count.set(sum);
        context.write(ans, count);
    }
}
