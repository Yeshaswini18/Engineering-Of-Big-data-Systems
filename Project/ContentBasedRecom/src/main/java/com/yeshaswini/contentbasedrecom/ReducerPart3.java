package com.yeshaswini.contentbasedrecom;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ReducerPart3 extends Reducer<Text, Text, Text, Text> {
    Text reduceKey = new Text();
    Text reduceValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double groupSize = 0;
        ArrayList<Text> cache = new ArrayList<Text>();
        double dotProduct = 0;
        double rating1Sum = 0;
        double rating2Sum = 0;
        double rating1NormSq = 0;
        double rating2NormSq = 0;
        double maxNumOfRaters1 = 0;
        double maxNumOfRaters2 = 0;

        for (Text aNum : values) {
            Text writable = new Text();
            writable.set(aNum);
            cache.add(writable);
            groupSize += 1;
        }

        int size = cache.size();
        for(int i = 0; i < size; i++) {
            String[] token = cache.get(i).toString().split(",", -1);
            dotProduct += Double.parseDouble(token[4]);
            rating1Sum += Double.parseDouble(token[0]);
            rating2Sum += Double.parseDouble(token[2]);
            rating1NormSq += Double.parseDouble(token[5]);
            rating2NormSq += Double.parseDouble(token[6]);
            double numberOfRaters1 = Double.parseDouble(token[1]);
            double numberOfRaters2 = Double.parseDouble(token[3]);
            if (numberOfRaters1 > maxNumOfRaters1) {
                maxNumOfRaters1 += numberOfRaters1;
            }
            if (numberOfRaters2 > maxNumOfRaters2) {
                maxNumOfRaters2 += numberOfRaters2;
            }
        }
        double pearson = calculatePearsonCorrelation(groupSize, dotProduct, rating1Sum, rating2Sum, rating1NormSq, rating2NormSq);
        double cosine = calculateCosineCorrelation(dotProduct, Math.sqrt(rating1NormSq), Math.sqrt(rating2NormSq));
        double jaccard = calculateJaccardCorrelation(groupSize, maxNumOfRaters1, maxNumOfRaters2);
        String reducerOutput = pearson + "," + cosine + "," + jaccard;

        reduceValue.set(reducerOutput);
        reduceKey.set(key);
        context.write(reduceKey, reduceValue);
    }

    static double calculatePearsonCorrelation ( double size, double dotProduct, double rating1Sum, double rating2Sum, double rating1NormSq, double rating2NormSq ) {
        double numerator = size * dotProduct - rating1Sum * rating2Sum;
        double denominator = Math.sqrt(size * rating1NormSq - rating1Sum * rating1Sum) * Math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum);
        return numerator / denominator;
    }

    static double calculateCosineCorrelation (double dotProduct, double rating1Norm, double rating2Norm) {
        return dotProduct / (rating1Norm * rating2Norm);
    }

    static double calculateJaccardCorrelation(double inCommon, double totalA, double totalB) {
        double union = totalA + totalB - inCommon;
        return inCommon / union;
    }
}
