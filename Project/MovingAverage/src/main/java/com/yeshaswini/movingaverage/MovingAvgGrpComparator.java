package com.yeshaswini.movingaverage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MovingAvgGrpComparator extends WritableComparator {
    protected MovingAvgGrpComparator() {
        super(MovieDateWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MovieDateWritable loc1 = (MovieDateWritable) a;
        MovieDateWritable loc2 = (MovieDateWritable) b;

        return loc1.getMovieID().compareTo(loc2.getMovieID());
    }
}
