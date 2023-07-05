import subprocess
import os

n_samples = [1000, 100000, 1000000]
n_features = [2, 5, 10]
tolerance = [0.01, 0.0001]
max_iter = 100
num_reducers = [1, 3]

# Itera su tutti i valori dei parametri

for tol in tolerance:
    for reducer in num_reducers:
        folder_path = './inputs/'  # Percorso della cartella contenente i file
        for file_name in os.listdir(folder_path):
            if file_name.startswith('input'):
                parts = file_name.split("_")    
                center = parts[2].split(".txt")[0] 

                strings = parts[1].split("x") 
                s = strings[0]
                f = strings[1]

                if tol == 0.0001:
                    output_file_name = f"output_{s}x{f}_{center}_{reducer}_min_tol"
                else:
                    output_file_name = f"output_{s}x{f}_{center}_{reducer}_max_tol"
                command = f"hadoop jar PKMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.ParallelKMeans {center} {tol} {max_iter} {reducer} inputs/{file_name} {output_file_name}"
                print(command)
                # Esegui il comando utilizzando subprocess
                subprocess.run(command, shell=True)

                output_mask = output_file_name+"/iter*"
                command = f"hadoop fs -rm -r {output_mask}"
                subprocess.run(command, shell=True)