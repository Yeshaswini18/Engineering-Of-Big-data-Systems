package com.yeshaswini.profitcalculation;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class ProfitMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\\|", -1);

        if(fields.length != 24){
            return;
        }

        String jsonStringGenre = fields[3];
        String jsonStringCompany = fields[12];



        if (jsonStringGenre.equals("genres") || jsonStringGenre.length() < 4 || jsonStringCompany.length() < 4) {
            return;
        }

        jsonStringGenre = jsonStringGenre.substring(2, jsonStringGenre.length() - 2);
        jsonStringGenre = jsonStringGenre.replaceAll("'", "\"");
        jsonStringGenre = jsonStringGenre.replaceAll("}, ", "}|");
        String[] values = jsonStringGenre.split("\\|", -1);
        JSONParser jsonParser = new JSONParser();

        for (String val : values) {
            try {
                JSONObject jsonObject = (JSONObject) jsonParser.parse(val);
                String isGenre = (String) jsonObject.get("name");
                if(isGenre.equals("Crime")){
                    companyProfit(context, fields, jsonStringCompany);
                }
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    public void companyProfit(Context context, String[] fields, String jsonStringCompany) {
        Text company = new Text();
        LongWritable profit = new LongWritable();

        long budget = Long.parseLong(fields[2]);
        long revenue = Long.parseLong(fields[15]);
        long profitMade = revenue - budget;

        jsonStringCompany = jsonStringCompany.substring(2, jsonStringCompany.length() - 2);
        jsonStringCompany = jsonStringCompany.replaceAll("'", "\"");
        jsonStringCompany = jsonStringCompany.replaceAll("}, ", "}|");
        String[] values = jsonStringCompany.split("\\|", -1);
        JSONParser jsonParser = new JSONParser();

        for (String val : values) {
            try {
                JSONObject jsonObject = (JSONObject) jsonParser.parse(val);
                String companyName = (String) jsonObject.get("name");
                company.set(companyName);
                profit.set(profitMade);
                context.write(company, profit);
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
