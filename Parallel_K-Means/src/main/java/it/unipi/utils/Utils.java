package it.unipi.utils;

import it.unipi.hadoop.CentroidWritable;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utils {
    public static List<Double> sum(List<Double> sum, List<Double> values) {
        if(sum.isEmpty()){
            return values;
        }else{
            for (Double dim_sum:sum) {
                int index = sum.indexOf(dim_sum);
                dim_sum += values.get(index);
                sum.set(index,dim_sum);
            }
            return sum;
        }
    }

    public static List<CentroidWritable> randomCentroids(int k, int d){

        List<CentroidWritable> centroids = new ArrayList<>();
        for(int i = 0; i<k; i++){
            CentroidWritable c = new CentroidWritable();
            for(int j=0; j<d; j++){
                c.addDimension(Math.random());
            }
            centroids.add(c);
        }
        return centroids;
    }

    public static boolean checkConvergence(List<CentroidWritable> old_centroids, List<CentroidWritable> new_centroids, double tolerance){
        for(CentroidWritable c : new_centroids){
            int index = new_centroids.indexOf(c);
            double distance = c.distance(old_centroids.get(index));
            if(distance > tolerance)
                return false;
        }
        return true;
    }
    /*
    public static List<CentroidWritable> readCentroids(String filePath) {
        DataInputStream dataInputStream = null;
        CentroidWritable c = new CentroidWritable();
        try {
            FileInputStream fileInputStream = new FileInputStream(filePath);
            dataInputStream = new DataInputStream(fileInputStream);
            c.readFields(dataInputStream);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    */
}
