package com.yeshaswini.toptencompanies;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopTenCompaniesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private TreeMap<Double, String> tmap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tmap = new TreeMap<Double, String>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|", -1);

        if(fields.length != 24){
            return;
        }

        String jsonStringCompany = fields[12];


        if (jsonStringCompany.equals("production_companies") || jsonStringCompany.length() < 4) {
            return;
        }
        double vote_average = Double.parseDouble(fields[22]);

        jsonStringCompany = jsonStringCompany.substring(2, jsonStringCompany.length() - 2);
        jsonStringCompany = jsonStringCompany.replaceAll("'", "\"");
        jsonStringCompany = jsonStringCompany.replaceAll("}, ", "}|");
        String[] values = jsonStringCompany.split("\\|", -1);
        JSONParser jsonParser = new JSONParser();

        for (String val : values) {
            try {
                JSONObject jsonObject = (JSONObject) jsonParser.parse(val);
                String companyName = (String) jsonObject.get("name");
                tmap.put(vote_average, companyName);
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            }
        }
        if (tmap.size() > 10)
        {
            tmap.remove(tmap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double, String> entry : tmap.entrySet())
        {
            double vote_average = entry.getKey();
            String name = entry.getValue();

            context.write(new Text(name), new DoubleWritable(vote_average));
        }
    }
}
