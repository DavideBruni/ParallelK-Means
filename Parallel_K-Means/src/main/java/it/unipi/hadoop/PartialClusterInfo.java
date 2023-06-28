package it.unipi.hadoop;

import it.unipi.utils.Utils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PartialClusterInfo implements WritableComparable<PartialClusterInfo> {

    private List<Double> partial_sum = new ArrayList<>();
    private int num_points = 0;

    public List<Double> getPartial_sum() {
        return partial_sum;
    }

    public int getNum_points() {
        return num_points;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public int compareTo(PartialClusterInfo o) {

        // conviene usare il metodo distance e qualche if

        return 0;
    }

    public void add_point(Point point) {
        partial_sum = Utils.sum(partial_sum,point.getValues());
        num_points++;
    }

    public void add_points(PartialClusterInfo pi) {
        partial_sum = Utils.sum(partial_sum,pi.getPartial_sum());
        num_points += pi.getNum_points();
    }
}
