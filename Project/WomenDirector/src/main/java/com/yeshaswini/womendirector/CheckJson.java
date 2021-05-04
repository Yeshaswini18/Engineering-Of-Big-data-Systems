package com.yeshaswini.womendirector;


import org.codehaus.jettison.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class CheckJson {
    public static void main(String[] args) throws JSONException, ParseException {
        String jsonString = "[{'id': 931, 'name': 'jealousy'}, {'id': 4290, 'name': 'toy'}]";
        jsonString = jsonString.substring(1, jsonString.length() - 1);
        jsonString = jsonString.replaceAll("'", "\"");
        jsonString = jsonString.replaceAll("}, ", "}|");
        String[] values = jsonString.split("\\|", -1);
        for (String value : values) {
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(value);
            System.out.println(jsonObject);
        }
    }
}
