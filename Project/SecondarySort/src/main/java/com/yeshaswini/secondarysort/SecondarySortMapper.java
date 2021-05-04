package com.yeshaswini.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKeyClass, IntWritable> {

    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\\|", -1);

        if(fields.length != 24){
            return;
        }
        String jsonString = fields[12];
        String title = fields[8];
        String date = fields[14];


        if (jsonString.equals("production_companies") || jsonString.length() < 4 || title.length() == 0 || date.length() != 10) {
            return;
        }

        String year = date.substring(0,4);

        jsonString = jsonString.substring(2, jsonString.length() - 2);
        jsonString = jsonString.replaceAll("'", "\"");
        jsonString = jsonString.replaceAll("}, ", "}|");
        String[] values = jsonString.split("\\|", -1);
        JSONParser jsonParser = new JSONParser();

        for (String val : values) {
            try {
                JSONObject jsonObject = (JSONObject) jsonParser.parse(val);
                String companyName = (String) jsonObject.get("name");
                context.write(new CompositeKeyClass(companyName, year), one);
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            }
        }
    }
}
