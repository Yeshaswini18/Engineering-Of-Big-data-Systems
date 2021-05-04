package com.yeshaswini.movingaverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MovingAvgPart1Mapper extends Mapper<LongWritable, Text, MovieDateWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        MovieDateWritable movieDateWritable = new MovieDateWritable();
        String line = value.toString();
        String[] fields = line.split(",");
        if (!fields[1].equals("movieId")) {
            String movieID = fields[1];
            String timestamp = fields[3];
            Date d = new Date(Long.parseLong(timestamp)*1000);
            DateFormat f = new SimpleDateFormat("yyyyMMdd");
            String date = f.format(d);

            movieDateWritable.setMovieID(new Text(movieID));
            movieDateWritable.setDate(new IntWritable(Integer.parseInt(date)));
            String value1 = date + "\t" + fields[2];
            context.write(movieDateWritable, new Text(value1));
        }
    }
}
