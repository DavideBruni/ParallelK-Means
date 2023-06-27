package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartialClusterInfo implements WritableComparable<PartialClusterInfo> {

    private double partial_sum = 0.0d;
    private int num_points = 0;

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public int compareTo(PartialClusterInfo o) {
        return 0;
    }

    public void add_point(Point point) {
        // partial_sum+=distance;       // TODO
        num_points++;
    }
}
