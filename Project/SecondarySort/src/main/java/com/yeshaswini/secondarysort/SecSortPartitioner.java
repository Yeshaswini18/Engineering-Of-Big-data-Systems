package com.yeshaswini.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecSortPartitioner extends Partitioner<CompositeKeyClass, IntWritable> {
    @Override
    public int getPartition(CompositeKeyClass key, IntWritable o2, int numOfPartitions) {
        return key.getProductionCompany().hashCode() % numOfPartitions;
    }
}
