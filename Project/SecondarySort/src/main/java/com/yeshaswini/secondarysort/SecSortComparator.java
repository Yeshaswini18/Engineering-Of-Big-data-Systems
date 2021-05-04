package com.yeshaswini.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecSortComparator extends WritableComparator {

    protected SecSortComparator() {
        super(CompositeKeyClass.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyClass key1 = (CompositeKeyClass) a;
        CompositeKeyClass key2 = (CompositeKeyClass) b;
        return key1.getYear().compareTo(key2.getYear());
    }
}
