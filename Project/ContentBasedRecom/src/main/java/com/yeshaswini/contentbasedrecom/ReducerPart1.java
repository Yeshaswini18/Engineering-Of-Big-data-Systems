package com.yeshaswini.contentbasedrecom;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class ReducerPart1 extends Reducer<Text, Text, Text, Text> {
    Text keyOut = new Text();
    Text valueOut = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long numberOfRaters = 0;
        ArrayList<Text> cache = new ArrayList<Text>();

        for (Text aNum : values) {
            Text writable = new Text();
            writable.set(aNum);
            cache.add(writable);
            numberOfRaters += 1;
        }

        int size = cache.size();
        for (int i = 0; i < size; ++i) {
            Text value = cache.get(i);
            String[] tokens = value.toString().split(",", -1);

            keyOut.set(tokens[0]);
            String valueOut1 = key + "," + tokens[1] + "," + numberOfRaters;
            valueOut.set(valueOut1);
            context.write(keyOut,valueOut);
        }
    }
}
