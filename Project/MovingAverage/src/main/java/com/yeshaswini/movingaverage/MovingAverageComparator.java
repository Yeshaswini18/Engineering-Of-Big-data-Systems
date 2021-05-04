package com.yeshaswini.movingaverage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MovingAverageComparator extends WritableComparator {
    protected MovingAverageComparator() {
        super(MovieDateWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MovieDateWritable mov1 = (MovieDateWritable) a;
        MovieDateWritable mov2 = (MovieDateWritable) b;

        int cmpresult = mov1.getMovieID().compareTo(mov2.getMovieID());
        if (cmpresult != 0) {

            return cmpresult;
        }
        return mov1.getDate().compareTo(mov2.getDate());
    }
}
