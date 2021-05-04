package com.yeshaswini.contentbasedrecom;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperPart2 extends Mapper<LongWritable, Text, Text, Text> {
    Text mapKey = new Text();
    Text mapValue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        mapKey.set(tokens[0]);
        mapValue.set(tokens[1]);
        context.write(mapKey, mapValue);
    }
}
