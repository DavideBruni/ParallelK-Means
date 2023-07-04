package it.unipi.utils;

import it.unipi.hadoop.CentroidWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static List<CentroidWritable> readCentroids(Configuration config, String inputPath) throws IOException {
        List<CentroidWritable> centroids = new ArrayList<>();
        Path dirPath = new Path(inputPath);
        PathFilter filter = new PathFilter() {
            public boolean accept(Path path) {
                return path.getName().startsWith("part-r");
            }
        };

        FileSystem fs = FileSystem.get(config);
        FileStatus[] fileStatuses = fs.listStatus(dirPath, filter);
        for (FileStatus status : fileStatuses) {
            Path filePath = status.getPath();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String patternString = "\\[(.*?)\\]";
                    Pattern pattern = Pattern.compile(patternString);
                    Matcher matcher = pattern.matcher(line);

                    if (matcher.find()) {
                        CentroidWritable c = new CentroidWritable(matcher.group(1));
                        centroids.add(c);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return centroids;
    }

}
