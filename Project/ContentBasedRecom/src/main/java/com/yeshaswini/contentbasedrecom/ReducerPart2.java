package com.yeshaswini.contentbasedrecom;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReducerPart2 extends Reducer<Text, Text, Text, Text> {
    Text reduceKey = new Text();
    Text reduceValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<List<List<String>>> combinations = new ArrayList<>();
        List<Text> list = new ArrayList<>();

        for (Text value : values) {
            list.add(new Text(value));
        }

        if (list.size() < 2) {
            context.write(key, new Text(String.valueOf(list.size())));
            return;
        }

        //create combinations of movies
        for(int i = 0; i < list.size(); i++) {
            List<String> tokens1 = Arrays.asList(list.get(i).toString().split(",", -1));
            for(int j = i+1; j < list.size(); j++) {
                List<String> tokens2 = Arrays.asList(list.get(j).toString().split(",", -1));
                List<List<String>> combination = new ArrayList<>();
                combination.add(tokens1);
                combination.add(tokens2);
                combinations.add(combination);
            }
        }

        for(List<List<String>> combination: combinations) {
            double rating1 = Double.parseDouble(combination.get(0).get(1));
            double rating2 = Double.parseDouble(combination.get(1).get(1));

            long noOfRaters1 = Long.parseLong(combination.get(0).get(2));
            long noOfRaters2 = Long.parseLong(combination.get(1).get(2));

            double ratingProduct = rating1 * rating2;
            double rating1Squared = rating1 * rating1;
            double rating2Squared = rating2 * rating2;

            String rKey = combination.get(0).get(0) + "," + combination.get(1).get(0);
            String rValue = rating1 + "," + noOfRaters1 + "," + rating2 + "," + noOfRaters2 + "," + ratingProduct + "," + rating1Squared + "," + rating2Squared;

            reduceKey.set(rKey);
            reduceValue.set(rValue);

            context.write(reduceKey, reduceValue);
        }
    }
}
