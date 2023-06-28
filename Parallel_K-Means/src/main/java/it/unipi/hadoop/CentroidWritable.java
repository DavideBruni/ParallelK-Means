package it.unipi.hadoop;

import java.util.ArrayList;
import java.util.List;

public class CentroidWritable extends Point{

    public CentroidWritable(String str) {   //input format "x,y,z"
        super(str);
    }

    public CentroidWritable(PartialClusterInfo partialClusterInfo) {
        super();
        List<Double> values = new ArrayList<>();
        int num_points = partialClusterInfo.getNum_points();
        for(Double dir_sum : partialClusterInfo.getPartial_sum()){
            values.add(dir_sum/num_points);
        }
        setValues(values);
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
