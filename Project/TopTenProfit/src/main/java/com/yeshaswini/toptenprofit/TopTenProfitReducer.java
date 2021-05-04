package com.yeshaswini.toptenprofit;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopTenProfitReducer extends Reducer<Text, LongWritable, LongWritable, Text> {
    private TreeMap<Long, String> tmap2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tmap2 = new TreeMap<Long, String>();
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        String name = key.toString();
        long profit = 0;

        for (LongWritable val : values)
        {
            profit = val.get();
        }

        tmap2.put(profit, name);

        if (tmap2.size() > 10)
        {
            tmap2.remove(tmap2.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : tmap2.entrySet())
        {
            long profit = entry.getKey();
            String name = entry.getValue();
            context.write(new LongWritable(profit), new Text(name));
        }
    }
}
