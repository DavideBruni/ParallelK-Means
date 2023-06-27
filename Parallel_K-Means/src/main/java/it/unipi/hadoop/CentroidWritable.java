package it.unipi.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CentroidWritable extends Point implements WritableComparable<CentroidWritable> {

    public CentroidWritable(String str) {   //input format "x,y,z"
        super(str);
    }

    @Override
    public int compareTo(CentroidWritable o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public String toString() {
        return ""+getValues();
    }

    public double distance(Point point) {
        double sum = 0;
        List<Double> distance_points = point.getValues();
        for(double d : distance_points){
            sum +=Math.pow(getValues().get(distance_points.indexOf(d)) - d,2);      //(Ci - Pi)^2
        }
        return Math.sqrt(sum);
    }
}
