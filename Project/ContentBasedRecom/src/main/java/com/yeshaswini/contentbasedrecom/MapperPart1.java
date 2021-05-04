package com.yeshaswini.contentbasedrecom;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperPart1 extends Mapper<LongWritable, Text, Text, Text> {
    Text movieID = new Text();
    Text tuple = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",", -1);

        String outValue = tokens[0] + "," + tokens[2];
        String ID = tokens[1];
        movieID.set(ID);
        tuple.set(outValue);
        context.write(movieID, tuple);
    }
}
