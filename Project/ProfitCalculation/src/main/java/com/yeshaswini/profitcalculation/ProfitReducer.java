package com.yeshaswini.profitcalculation;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProfitReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        LongWritable profit = new LongWritable();
        long totalProfit = 0;
        for(LongWritable val: values) {
            long value = Long.parseLong(String.valueOf(val));
            totalProfit += value;
        }
        profit.set(totalProfit);
        context.write(key, profit);
    }
}
