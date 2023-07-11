package it.unipi.utils;

import it.unipi.hadoop.CentroidWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static List<Double> sum(List<Double> sum, List<Double> values) {
        if (sum.isEmpty()) {
            return values;
        } else {
            List<Double> result = new ArrayList<>(sum.size());
            for (int i = 0; i < sum.size(); i++) {
                double dim_sum = sum.get(i);
                double dim_value = values.get(i);
                result.add(dim_sum + dim_value);
            }
            return result;
        }
    }

    public static boolean checkConvergence(List<CentroidWritable> old_centroids, List<CentroidWritable> new_centroids, double tolerance){
        double distance_sum = 0.0d;
        for(int i =0; i< new_centroids.size(); i++){
            distance_sum += new_centroids.get(i).distance(old_centroids.get(i));
            if(distance_sum > tolerance)
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
        TreeMap<Integer, CentroidWritable> centroids = new TreeMap<>();

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
                        String[] parts = line.split("\\s+", 2);
                        if (parts.length == 2) {
                            int clusterIndex = Integer.parseInt(parts[0]);
                            centroids.put(clusterIndex, c);
                        }
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return new ArrayList<>(centroids.values());
    }



    public static void saveInfo(int k, int iter, double tolerance, int numReducers, String inputPath, List<CentroidWritable> centroids, long startTime, long endTime, String outputPath, boolean flag) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);

            StringBuilder infoBuilder = new StringBuilder();
            infoBuilder.append("k: ").append(k).append("\n");
            infoBuilder.append("iter: ").append(iter).append("\n");
            infoBuilder.append("tolerance: ").append(tolerance).append("\n");
            infoBuilder.append("numReducers: ").append(numReducers).append("\n");
            infoBuilder.append("inputPath: ").append(inputPath).append("\n");
            infoBuilder.append("centroids: ").append(centroids).append("\n");
            infoBuilder.append("startTime: ").append(startTime).append("\n");
            infoBuilder.append("endTime: ").append(endTime).append("\n");
            infoBuilder.append("executionTime: ").append(endTime-startTime).append("\n");

            Path filePath = new Path(outputPath+ "/results.txt");
            FSDataOutputStream outputStream = fs.create(filePath);

            outputStream.writeBytes(infoBuilder.toString());

            outputStream.close();
            
            Path file_csv_path;
            if(flag)
                file_csv_path = new Path("results.csv");
            else
                file_csv_path = new Path("results_combiner.csv");
            FSDataOutputStream outputStream_csv;
            if(!fs.exists(file_csv_path)){
                outputStream_csv = fs.create(file_csv_path);
                outputStream_csv.writeBytes("k,iter,tolerance,numReducers,inputPath,executionTime\n"+k+','+iter+','+tolerance+','+numReducers+','+inputPath+','+(endTime-startTime)/1000+'\n');
            }else{
                outputStream_csv = fs.append(file_csv_path);
                outputStream_csv.writeBytes(""+k+','+iter+','+tolerance+','+numReducers+','+inputPath+','+(endTime-startTime)/1000+'\n');
            }
            outputStream_csv.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
