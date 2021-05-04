package com.yeshaswini.movingaverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieDateWritable implements WritableComparable<MovieDateWritable> {
    private Text MovieID;
    private IntWritable Date;

    public MovieDateWritable() {
        set(new Text(),new IntWritable());
    }

    private void set(Text MovieID, IntWritable Date) {
        this.MovieID=MovieID;
        this.Date=Date;
    }

    public Text getMovieID() {
        return MovieID;
    }

    public IntWritable getDate() {
        return Date;
    }

    public void setMovieID(Text MovieID) {
        this.MovieID = MovieID;
    }

    public void setDate(IntWritable Date) {
        this.Date = Date;

    }



    @Override
    public int compareTo(MovieDateWritable o) {
        int cmp = this.MovieID.compareTo(o.getMovieID());
        if (cmp!=0) {
            return cmp;
        }
        return this.Date.compareTo(o.getDate());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        MovieID.write(dataOutput);
        Date.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        MovieID.readFields(dataInput);
        Date.readFields(dataInput);
    }

}
