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

/**

 The Utils class provides utility methods used in the K-means algorithm.
 */

public class Utils {
    /**
     * Calculates the sum of two lists of values element-wise.
     * If the first list is empty, the second list is returned as is.
     *
     * @param sum    The first list of values.
     * @param values The second list of values.
     * @return The sum of the two lists of values.
     */
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


    /**
     * Checks the convergence of the K-means algorithm by comparing the distances between the old and new centroids.
     * If the sum of distances is less than or equal to the tolerance, it indicates convergence.
     *
     * @param old_centroids The old centroids.
     * @param new_centroids The new centroids.
     * @param tolerance     The tolerance threshold.
     * @return True if the centroids have converged, false otherwise.
     */
    public static boolean checkConvergence(List<CentroidWritable> old_centroids, List<CentroidWritable> new_centroids, double tolerance){
        double distance_sum = 0.0d;
        for(int i =0; i< new_centroids.size(); i++){
            distance_sum += new_centroids.get(i).distance(old_centroids.get(i));
            if(distance_sum > tolerance)
                return false;
        }

        return true;
    }


    /**
     * Generates a list of random centroids from the beginning of the input file.
     * The first k lines of the file are read to obtain the initial centroids.
     *
     * @param k        The number of centroids.
     * @param filePath The path to the input file.
     * @return A list of random centroids.
     */
    /* We took the first k centroids because:
        - Random selection would have been costly when the file occupies more than one block.
        - It is faster to read the first lines than to generate a random number and then read the lines.
        - Choosing random values outside the dataset would have been incorrect as they could have generated coordinates
          outside the distribution (outliers).
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


    /**
     * Reads the final centroids from the output files of the MapReduce job.
     *
     * @param config     The Hadoop configuration.
     * @param inputPath  The path to the input directory.
     * @return A list of final centroids.
     * @throws IOException If an I/O error occurs.
     */
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


    /**
     * Saves the information about the K-means algorithm execution to a text file.
     *
     * @param k           The number of clusters.
     * @param iter        The number of iterations.
     * @param tolerance   The tolerance threshold.
     * @param numReducers The number of reducers.
     * @param inputPath   The input path.
     * @param centroids   The final centroids.
     * @param startTime   The start time of the execution.
     * @param endTime     The end time of the execution.
     * @param outputPath  The output path.
     * @param flag        A flag indicating if it's the results from a combiner-based implementation (if false, it is).
     */
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
