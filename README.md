# ParallelKmeans

This project has been developed as part of the Cloud Computing course at the Master's Degree in Artificial Intelligence and Data Engineering at the University of Pisa during the Accademic Year 2022-2023.

## Introduction
ParallelKmeans is a parallel implementation of the K-means algorithm using the MapReduce model. The K-means algorithm is widely used in the field of unsupervised learning to cluster data based on their similarity.

## Parallel K-means Algorithm
The parallel K-means algorithm leverages distributed computing power provided by the MapReduce model to improve performance and handle large volumes of data. In this context, the K-means algorithm is executed in a distributed manner across multiple nodes, reducing the overall execution time.

## MapReduce Model
The MapReduce model is a programming paradigm that allows processing large amounts of data in a scalable and parallel way on a cluster of computers. In this context, the K-means process is divided into two main phases: mapping (mapper) and reducing (reducer).

### Mapper with Combiner
In the mapping phase, the K-means algorithm uses the "Combiner" technique to reduce the amount of data transmitted between the mapper and the reducer. The Combiner performs partial aggregation of data within the mapper itself, thereby reducing network traffic and improving overall performance.

#### Pseudo-code for Mapper with Combiner
```python
Input: Centroid List C, Input Split I
Output: Key-value pair (cluster_index, partial_cluster_info)

procedure Initialize
    # Initialize the list of partial cluster info
    P_C = InitializePartialClusterInfo()

procedure Map(key, p)
    # Assign each point to the nearest centroid and update the partial info
    nearest = indexNearestCentroid(C)
    P_C[nearest].p_sum += p
    P_C[nearest].p_num += 1

procedure Close
    # Emit the pair for each cluster
    for i, pc in P_C:
        Emit(i, pc)
```

### Reducer with Combiner
In the reducing phase, the partial results from the mappers are processed to compute the new centroids of the clusters.

#### Pseudo-code for Reducer with Combiner
```python
Input: Cluster Index: i, List of PartialClusterInfo P_C assigned to cluster i
Output: Key-value pair (cluster_index, new_centroid)

procedure Reduce(i, PC)
    # Compute the sum of partial cluster info for each cluster
    sum = 0
    n = 0
    for pc in P_C:
        sum += pc.sum
        n += pc.num
    new_centroid = sum / n
    Emit(i, new_centroid)
```

## ParallelKMeansWithCombiner Class
In the `ParallelKMeansWithCombiner` class, the same parallel K-means algorithm is implemented, but with the addition of the `Combiner` class to handle large-sized files. The usage of the `Combiner` class allows for data processing of BIG dataset, because the RAM memory is not unlimited, and the techinque of In-Mapper combining is faster, but use also a lot of memory.

# Running the Code on Hadoop

To execute the code in the ParallelKMeans repository, you will need to run it on Hadoop. The following commands show how to launch the code using different versions depending on whether you want to use the In-Mapper Combiner or the regular Combiner approach:

For In-Mapper Combiner version:
```bash
hadoop jar PKMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.ParallelKMeans {center} {tol} {max_iter} {reducer} inputs/{file_name} {output_dir_name}
```

For regular Combiner version:
```bash
hadoop jar PKMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.ParallelKMeansWithCombiner {center} {tolerance} {max_iter} {reducer} inputs/{file_name} {output_dir_name}
```

In both commands, replace the following placeholders with the appropriate values:

- `{center}`: The number of centroids for the K-means algorithm.
- `{tol}` or `{tolerance}`: The tolerance value for convergence.
- `{max_iter}`: The maximum number of iterations.
- `{reducer}`: The number of reducers to be used.
- `{file_name}`: The name of the input file or directory containing the data.
- `{output_dir_name}`: The name of the output directory to store the results.

Make sure to have Hadoop installed and properly configured on your system before running these commands. Adjust the class name `it.unipi.hadoop.ParallelKMeans` or `it.unipi.hadoop.ParallelKMeansWithCombiner` according to the class you want to execute.

Feel free to modify the command arguments based on your specific requirements. Execute the command in the root directory of your Hadoop cluster or provide the full HDFS path for the input and output files.

For any further details or assistance, please refer to the project documentation or consult the relevant resources on executing Hadoop jobs.

# License

The code in the ParallelKMeans repository is released under the [MIT License](https://opensource.org/licenses/MIT). 

This means that you are free to use, modify, and distribute the code in this repository, both for personal and commercial purposes, as long as the license terms are respected. The license ensures that you have the freedom to adapt the code to your needs and contribute to its improvement if desired.
