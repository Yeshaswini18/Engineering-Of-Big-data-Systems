package com.yeshaswini.toptenprofit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopTenProfitMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private TreeMap<Long, String> tmap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tmap = new TreeMap<Long, String>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|", -1);

        if(fields.length != 24){
            return;
        }

        String movie_name = fields[8];

        if (movie_name.equals("original_title") ) {
            return;
        }
        long profit = Long.parseLong(fields[15]) - Long.parseLong(fields[2]);

        tmap.put(profit, movie_name);

        if (tmap.size() > 10)
        {
            tmap.remove(tmap.firstKey());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : tmap.entrySet())
        {
            long profit = entry.getKey();
            String name = entry.getValue();

            context.write(new Text(name), new LongWritable(profit));
        }
    }
}
