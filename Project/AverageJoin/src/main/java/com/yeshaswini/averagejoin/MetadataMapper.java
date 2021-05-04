package com.yeshaswini.averagejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] values = data.split("\\|",-1);

        if (values.length == 24 && !values[5].equals("id") && !values[8].equals("original_title")) {
            context.write(new Text(values[5]), new Text("T"+values[8]));
        }

    }
}
