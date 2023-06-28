package it.unipi.hadoop;

import java.io.IOException;
import java.util.*;

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
            String[] single_centroid_str = centroids_str.split("],");           // ogni centroide è rappresentato da un array

            for (String s:single_centroid_str) {
                s = s.substring(1);         //tolgo la prima quadra
                centroids.add(new CentroidWritable(s));
            }

            // inizializzazione array di partial info
            // int k = context.getConfiguration().getInt("parallel.kmeans.k",2);

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
        if (otherArgs.length != 7) {
            System.err.println("Usage: MovingAverage <k number of clusters> <n number of elements> <d dimension> <tolerance> <max_iter> <input> <output>");
            System.exit(1);
        }
        
        Job job = Job.getInstance(conf, "ParallelKMeans");
        job.setJarByClass(ParallelKMeans.class);

        // set mapper/reducer
        job.setMapperClass(ParallelKMeansMapper.class);
        job.setReducerClass(ParallelKMeansReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CentroidWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set window size for moving average calculation
        int windowSize = Integer.parseInt(otherArgs[0]);
        job.getConfiguration().setInt("moving.average.window.size", windowSize);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

