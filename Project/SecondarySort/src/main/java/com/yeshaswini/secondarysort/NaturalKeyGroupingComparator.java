package com.yeshaswini.secondarysort;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {

    protected NaturalKeyGroupingComparator() {
        super(CompositeKeyClass.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyClass key1 = (CompositeKeyClass) a;
        CompositeKeyClass key2 = (CompositeKeyClass) b;
        return key1.getProductionCompany().compareTo(key2.getProductionCompany());
    }
}
