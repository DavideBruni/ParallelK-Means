package it.unipi.utils;

import it.unipi.hadoop.CentroidWritable;

import java.io.*;
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

    public static boolean checkConvergence(List<CentroidWritable> old_centroids, List<CentroidWritable> new_centroids, double tolerance){
        for(CentroidWritable c : new_centroids){
            int index = new_centroids.indexOf(c);
            double distance = c.distance(old_centroids.get(index));
            if(distance > tolerance)
                return false;
        }
        return true;
    }


    /* abbiamo preso i primi k perchè:
        - farlo randomico, sarebbe stato pesante nel momento in cui il file occupa più di un blocco
        - computazione più veloce (leggere le prime righe è più veloce di dover generare un numero casuale e poi leggere le righe)
        - prendere valori randomici al di fuori dal dataset sarebbe stato scorretto in quanto potevano essere generate coordinate
            fuori distribuzione (outliers)
      */
    public static List<CentroidWritable> randomCentroids(int k, String filePath){
        List<CentroidWritable> centroids = new ArrayList<>();

        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null && centroids.size() < k) {
                CentroidWritable c = new CentroidWritable(line);
                centroids.add(c);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return centroids;
    }

}
