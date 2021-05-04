package com.yeshaswini.genrefilter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class GenreFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text movieName = new Text();
    Text genre = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] fields = data.split("\\|", -1);

        if(fields.length != 24){
            return;
        }

        String jsonString = fields[3];

        if (jsonString.equals("genres") || jsonString.length() < 4) {
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
                String isGenre = (String) jsonObject.get("name");
                if(isGenre.equals("Animation")){
                    movieName.set(fields[8]);
                    genre.set(isGenre);
                    context.write(movieName, genre);
                }
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            }
        }
    }
}
