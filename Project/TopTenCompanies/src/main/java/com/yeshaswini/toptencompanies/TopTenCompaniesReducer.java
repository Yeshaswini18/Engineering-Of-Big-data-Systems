package com.yeshaswini.toptencompanies;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopTenCompaniesReducer extends Reducer<Text, DoubleWritable, DoubleWritable, Text> {
    private TreeMap<Double, String> tmap2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tmap2 = new TreeMap<Double, String>();
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String name = key.toString();
        double vote_average = 0;

        for (DoubleWritable val : values)
        {
            vote_average = val.get();
        }

        tmap2.put(vote_average, name);

        if (tmap2.size() > 10)
        {
            tmap2.remove(tmap2.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double, String> entry : tmap2.entrySet())
        {

            double profit = entry.getKey();
            String name = entry.getValue();
            context.write(new DoubleWritable(profit), new Text(name));
        }
    }
}
