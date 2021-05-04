package com.yeshaswini.numsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Summarization implements Writable {
    private double minimum;
    private double maximum;
    private long count;
    private double average;
    private double standardDeviation;
    private double median;

    public Summarization(double minimum, double maximum, long count, double average, double standardDeviation, double median) {
        this.minimum = minimum;
        this.maximum = maximum;
        this.average = average;
        this.count = count;
        this.standardDeviation = standardDeviation;
        this.median = median;
    }

    public Summarization() {
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

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public void setStandardDeviation(double standardDeviation) {
        this.standardDeviation = standardDeviation;
    }

    public double getMedian() {
        return median;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public String toString() {
        return "Summarization{" +
                "minimum=" + minimum +
                ", maximum=" + maximum +
                ", count=" + count +
                ", average=" + average +
                ", standardDeviation=" + standardDeviation +
                ", median=" + median +
                '}';
    }
}
