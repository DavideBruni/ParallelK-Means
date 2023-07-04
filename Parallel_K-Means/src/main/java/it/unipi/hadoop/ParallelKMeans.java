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

public class ParallelKMeans
{
    public static class ParallelKMeansMapper extends Mapper<LongWritable, Text, IntWritable, PartialClusterInfo>
    {

        private List<CentroidWritable> centroids = new ArrayList();
        private Map<Integer,List<Point>> points_to_cluster = new HashMap<>();

        public void setup(Mapper.Context context) throws IOException, InterruptedException
        {
            // inizializzazione array di centroidi
            String centroids_str = context.getConfiguration().get("parallel.kmeans.centroids",null);
            centroids_str = centroids_str.substring(1, centroids_str.length() - 2);     //rimuovo la prima e l'ultima quadra
            String[] single_centroid_str = centroids_str.split("], ");           // ogni centroide è rappresentato da un array

            for (String s:single_centroid_str) {
                s = s.substring(1);         //tolgo la prima quadra
                centroids.add(new CentroidWritable(s));
            }

        }

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

            // se non esiste una lista per il cluster cluster_index, inizializza la lista
            points_to_cluster.computeIfAbsent(cluster_index, k -> new ArrayList<>());

            points_to_cluster.get(cluster_index).add(point);

        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, IntWritable, PartialClusterInfo>.Context context) throws IOException, InterruptedException {
            for(Integer index : points_to_cluster.keySet()){
                List<Point> points = points_to_cluster.get(index);
                PartialClusterInfo p_info = new PartialClusterInfo();
                for(Point p : points){
                    p_info.add_point(p);
                }
                context.write(new IntWritable(index),p_info);
            }
        }
    }

    public static class ParallelKMeansReducer extends Reducer<IntWritable, PartialClusterInfo, IntWritable, CentroidWritable>
    {

        public void reduce(IntWritable key, Iterable<PartialClusterInfo> values, Context context) throws IOException, InterruptedException
        {
            PartialClusterInfo partialClusterInfo = new PartialClusterInfo();
            for (PartialClusterInfo pi : values) {
                partialClusterInfo.add_points(pi);
            }
            context.write(key,new CentroidWritable(partialClusterInfo));
        }

    }

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

        List<CentroidWritable> centroids = Utils.randomCentroids(k,inputPath);

        while (!converged && iteration<max_iter) {
            Job job = Job.getInstance(conf, "ParallelKMeans");
            job.setJarByClass(ParallelKMeans.class);
            job.setNumReduceTasks(numReducers);

            // set mapper/reducer
            job.setMapperClass(ParallelKMeansMapper.class);
            job.setReducerClass(ParallelKMeansReducer.class);

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
         System.exit(0);
    }
}

