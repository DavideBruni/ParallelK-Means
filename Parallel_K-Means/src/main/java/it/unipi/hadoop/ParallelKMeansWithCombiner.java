package it.unipi.hadoop;

import java.io.IOException;
import java.util.*;

import it.unipi.utils.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * The ParallelKMeansWithCombinerMapper class represents the mapper for the Parallel K-means algorithm with a combiner.
 */
public class ParallelKMeansWithCombiner
{
    public static class ParallelKMeansWithCombinerMapper extends Mapper<LongWritable, Text, IntWritable, PartialClusterInfo>
    {

        private List<CentroidWritable> centroids = new ArrayList();

        /**
         * Initializes the mapper by retrieving the centroids from the configuration.
         *
         * @param context The mapper context.
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the thread is interrupted.
         */
        public void setup(Mapper.Context context) throws IOException, InterruptedException
        {

            String centroids_str = context.getConfiguration().get("parallel.kmeans.centroids",null);
            centroids_str = centroids_str.substring(1, centroids_str.length() - 2);     // Remove the first and last square bracket
            String[] single_centroid_str = centroids_str.split("], ");           // each centroid is represented by an array

            for (String s:single_centroid_str) {
                s = s.substring(1);         //Remove the first square bracket
                centroids.add(new CentroidWritable(s));
            }

        }


        /**
         * Maps each input point to the nearest centroid, emit the cluster_index - point couple.
         *
         * @param key                   The input key (ignored).
         * @param value                 The input value representing a point as a Text.
         * @param context               The context object of the mapper.
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the thread is interrupted.
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            List<Double> distances = new ArrayList<>();
            String point_as_string = value.toString();
            Point point = new Point(point_as_string);

            for(CentroidWritable c : centroids) {
                distances.add(c.distance(point));
            }

            double min_distance = Collections.min(distances);
            int cluster_index = distances.indexOf(min_distance);

            // reducer e combiner richiedono lo stesso input, per questo viene mascherato da PartialClusterInfo un oggetto di tipo Point
            PartialClusterInfo out = new PartialClusterInfo();
            out.add_point(point);

            context.write(new IntWritable(cluster_index),out);
        }

    }

    /**
     * The ParallelKMeansWithCombinerReducer class represents the reducer for the Parallel K-means algorithm with a combiner.
     */
    public static class ParallelKMeansWithCombinerReducer extends Reducer<IntWritable, PartialClusterInfo, IntWritable, CentroidWritable>
    {
        /**
         * Combines the partial cluster information for each cluster and emits the final centroid for the cluster.
         *
         * @param key                   The cluster index.
         * @param values                The partial cluster information for the cluster.
         * @param context               The context object of the reducer.
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the thread is interrupted.
         */
        public void reduce(IntWritable key, Iterable<PartialClusterInfo> values, Context context) throws IOException, InterruptedException
        {
            PartialClusterInfo partialClusterInfo = new PartialClusterInfo();
            for (PartialClusterInfo pi : values) {
                partialClusterInfo.add_points(pi);
            }
            context.write(key,new CentroidWritable(partialClusterInfo));
        }

    }


    /**
     * The ParallelKMeansWithCombinerCombiner class represents the combiner for the Parallel K-means algorithm with a combiner.
     */
    public static class ParallelKMeansWithCombinerCombiner extends Reducer<IntWritable, PartialClusterInfo, IntWritable, PartialClusterInfo>
    {

        /**
         * Combines the intermediate data by merging the partial cluster information.
         *
         * @param key     The input key.
         * @param values  The input values.
         * @param context The combiner context.
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the thread is interrupted.
         */
        public void reduce(IntWritable key, Iterable<PartialClusterInfo> values, Context context) throws IOException, InterruptedException
        {
            PartialClusterInfo partialClusterInfo = new PartialClusterInfo();
            for (PartialClusterInfo p : values) {
                partialClusterInfo.add_points(p);
            }
            context.write(key,partialClusterInfo);
        }

    }

    /**
     * The main method of the ParallelKMeansWithCombiner class.
     * It configures and runs the MapReduce job for the parallel K-means algorithm with a combiner.
     *
     * @param args The command-line arguments.
     * @throws Exception If an exception occurs during the execution.
     */
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 6) {
            System.err.println("Usage: Parallel KMeans <k number of clusters> <tolerance> <max_iter> <r reducer number> <input> <output>");
            System.exit(1);
        }
        int iteration = 0;
        boolean converged = false;

        String inputPath = args[4];
        String outputPath = args[5];

        int k = Integer.parseInt(otherArgs[0]);
        int max_iter = Integer.parseInt(otherArgs[2]);
        double tolerance = Double.parseDouble(otherArgs[1]);
        int numReducers = Integer.parseInt(otherArgs[3]);

        long startTime = System.currentTimeMillis();

        List<CentroidWritable> centroids = Utils.randomCentroids(k,inputPath);
        while (!converged && iteration<max_iter) {
            Job job = Job.getInstance(conf, "ParallelKMeansWithCombiner");
            job.setJarByClass(ParallelKMeansWithCombiner.class);
            job.setNumReduceTasks(numReducers);

            // set mapper/reducer
            job.setMapperClass(ParallelKMeansWithCombinerMapper.class);
            job.setCombinerClass(ParallelKMeansWithCombinerCombiner.class);
            job.setReducerClass(ParallelKMeansWithCombinerReducer.class);

            // Set the types of the output key and value from the mappers
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(PartialClusterInfo.class);

            // define reducer's output key-value
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(CentroidWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            job.getConfiguration().set("parallel.kmeans.centroids", centroids.toString());

            FileOutputFormat.setOutputPath(job, new Path(outputPath + "/iteration_" + iteration));

            // Submit the job and wait for its completion
            boolean success = job.waitForCompletion(true);

            // Check the job status and exit accordingly
            if (!success) {
                System.exit(1);
            }

            // Check for convergence
            List<CentroidWritable> new_centroids = Utils.readCentroids(conf,outputPath + "/iteration_" + iteration);
            converged = Utils.checkConvergence(centroids, new_centroids,tolerance);

            centroids = new_centroids;
            iteration++;
        }
        long endTime = System.currentTimeMillis();
        Utils.saveInfo(k,iteration,tolerance,numReducers,inputPath,centroids,startTime,endTime,outputPath,false);
        System.exit(0);
    }
}

