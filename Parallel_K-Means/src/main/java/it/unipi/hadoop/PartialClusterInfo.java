package it.unipi.hadoop;

import it.unipi.utils.Utils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PartialClusterInfo implements Writable{

    private List<Double> partial_sum = new ArrayList<>();
    private int num_points = 0;

    /**
     * Retrieves the partial sum of values for the cluster.
     *
     * @return The list of partial sum values.
     */
    public List<Double> getPartial_sum() {
        return partial_sum;
    }

    /**
     * Retrieves the number of points assigned to the cluster.
     *
     * @return The number of points.
     */
    public int getNum_points() {
        return num_points;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(partial_sum.size());    // Write the size of the partial_sum list

        for (Double value : partial_sum) {
            dataOutput.writeDouble(value);          // Write each value in the partial_sum list
        }

        dataOutput.writeInt(num_points);            // Write the value of num_points
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        partial_sum.clear();                    // Clear the existing values in the partial_sum list
        int size = dataInput.readInt();         // Read the size of the partial_sum list

        for (int i = 0; i < size; i++) {        // Read each value from the data input and add it to the partial_sum list
            double value = dataInput.readDouble();
            partial_sum.add(value);
        }
        num_points = dataInput.readInt();       // Read the value of num_points
    }

    /**
     * Adds a point to the partial cluster information by updating the partial sum and incrementing the number of points.
     *
     * @param point The point to add.
     */
    public void add_point(Point point) {
        partial_sum = Utils.sum(partial_sum,point.getValues());
        num_points++;
    }

    /**
     * Combines the partial cluster information from another PartialClusterInfo object by updating the partial sum and incrementing the number of points.
     *
     * @param pi The PartialClusterInfo object to combine.
     */
    public void add_points(PartialClusterInfo pi) {
        partial_sum = Utils.sum(partial_sum,pi.getPartial_sum());
        num_points += pi.getNum_points();
    }
}
