package com.yeshaswini.numsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxKey implements Writable {
    private double minimum;
    private double maximum;
    private double sum;
    private double rating;

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    private long count;

    public MinMaxKey() {
    }

    public MinMaxKey(double minimum, double maximum, double sum, long count) {
        this.minimum = minimum;
        this.maximum = maximum;
        this.sum = sum;
        this.count = count;
    }

    public double getMinimum() {
        return minimum;
    }

    public void setMinimum(double minimum) {
        this.minimum = minimum;
    }

    public double getMaximum() {
        return maximum;
    }

    public void setMaximum(double maximum) {
        this.maximum = maximum;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(sum);
        dataOutput.writeDouble(minimum);
        dataOutput.writeDouble(maximum);
        dataOutput.writeLong(count);
        dataOutput.writeDouble(rating);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readLong();
        sum = dataInput.readDouble();
        maximum = dataInput.readDouble();
        minimum = dataInput.readDouble();
        rating= dataInput.readDouble();
    }
}
