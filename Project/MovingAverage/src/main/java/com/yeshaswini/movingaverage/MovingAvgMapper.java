package com.yeshaswini.movingaverage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class MovingAvgMapper extends Mapper<LongWritable, Text, MovieDateWritable, DoubleWritable> {
    private int N = 30;

    private Queue<Double> reading = new ArrayBlockingQueue<Double>(N);
    private String movieID ="";
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        if(!movieID.equals(fields[0])){
            movieID = fields[0];
            reading.clear();
        }
        reading.add(Double.valueOf(fields[1].toString()));

        if (reading.size() == N) {
            MovieDateWritable key1 = new MovieDateWritable();
            key1.setMovieID(new Text(fields[0]));
            key1.setDate(new IntWritable(Integer.parseInt(fields[1])));

            for (Iterator<Double> iterator = reading.iterator(); iterator.hasNext();){
                Double readin = iterator.next();
                context.write(key1, new DoubleWritable(readin));
            }
            reading.remove();
        }

    }
}
