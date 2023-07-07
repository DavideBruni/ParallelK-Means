import subprocess
import os

tolerance = 0.0001
max_iter = 100
num_reducers = [1, 3]
file_names=["./inputs/input_10000000x2_50.txt","./inputs/input_10000000x10_50.txt","./inputs/input_50000000x2_10.txt"]
# Itera su tutti i valori dei parametri

for reducer in num_reducers:
    folder_path = './inputs/'  # Percorso della cartella contenente i file
    for file_name in file_names:
        if file_name.startswith('input'):
            parts = file_name.split("_")    
            center = parts[2].split(".txt")[0] 

            strings = parts[1].split("x") 
            s = strings[0]
            f = strings[1]

            output_file_name = f"outputCombiner_{s}x{f}_{center}_{reducer}_min_tol"
            command = f"hadoop jar PKMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.ParallelKMeans {center} {tolerance} {max_iter} {reducer} inputs/{file_name} {output_file_name}"
            print(command)
            # Esegui il comando utilizzando subprocess
            subprocess.run(command, shell=True)

            output_mask = output_file_name+"/iter*"
            command = f"hadoop fs -rm -r {output_mask}"
            subprocess.run(command, shell=True)