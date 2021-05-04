package com.yeshaswini;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LanguageFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text title = new Text();
    Text language = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] values = data.split("\\|", -1);

        if(values.length == 24 && values[7].equals("en")){
            title.set(values[8]);
            language.set("English");
            context.write(title, language);
        }
    }
}
