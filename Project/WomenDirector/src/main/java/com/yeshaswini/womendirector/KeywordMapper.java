package com.yeshaswini.womendirector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class KeywordMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text wd = new Text();
    Text movieID = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String jsonString = value.toString();

        if (jsonString.equals("keywords") || jsonString.length() < 4) {
            return;
        }

        jsonString = jsonString.substring(2, jsonString.length() - 2);
        jsonString = jsonString.replaceAll("'", "\"");
        jsonString = jsonString.replaceAll("}, ", "}|");
        String[] values = jsonString.split("\\|", -1);
        JSONParser jsonParser = new JSONParser();

        for (String val : values) {
            try {
                JSONObject jsonObject = (JSONObject) jsonParser.parse(val);
                String isWomenDirector = (String) jsonObject.get("name");
                if(isWomenDirector.equals("woman director")){
                    movieID.set(String.valueOf(key));
                    wd.set("RWomen Director");
                    context.write(movieID, wd);
                }
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            }
        }
    }
}

