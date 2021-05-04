package com.yeshaswini.womendirector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text movieID = new Text();
    Text movieName = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] values = data.split("\\|",-1);
        if (values.length == 24 && !values[5].equals("id")){
            String name = "N" + values[8];
            movieID.set(values[5]);
            movieName.set(name);

            context.write(movieID, movieName);
        }
    }
}
