package org.example.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {

    public SortComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable firstWritableComparable, WritableComparable secondWritableComparable1) {
        var first = (DoubleWritable) firstWritableComparable;
        var second = (DoubleWritable) secondWritableComparable1;
        return second.compareTo(first);
    }
}
