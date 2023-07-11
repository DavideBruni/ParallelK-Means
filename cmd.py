import subprocess
import os

tolerance = [0.001, 0.00001]
max_iter = 100
num_reducers = [1, 3]
file_names=["input_10000000x10_10.txt","input_50000000x2_10.txt"]

# Itera su tutti i valori dei parametri
for tol in tolerance:
    for reducer in num_reducers:
        folder_path = './inputs/'  # Percorso della cartella contenente i file
        for file_name in os.listdir(folder_path):
            if file_name.startswith('input'):
                if file_name not in file_names:
                    parts = file_name.split("_")    
                    center = parts[2].split(".txt")[0] 

                    strings = parts[1].split("x") 
                    s = strings[0]
                    f = strings[1]

                    if tol == 0.00001:
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


file_names=["input_1000000x2_50.txt","input_10000000x10_10.txt","input_50000000x2_10.txt"]
tolerance = 0.00001
# Itera su tutti i valori dei parametri

for reducer in num_reducers:
    for file_name in file_names:
        parts = file_name.split("_")    
        center = parts[2].split(".txt")[0] 

        strings = parts[1].split("x") 
        s = strings[0]
        f = strings[1]

        output_file_name = f"outputCombiner_{s}x{f}_{center}_{reducer}_min_tol"
        command = f"hadoop jar PKMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.ParallelKMeansWithCombiner {center} {tolerance} {max_iter} {reducer} inputs/{file_name} {output_file_name}"
        print(command)
        # Esegui il comando utilizzando subprocess
        subprocess.run(command, shell=True)

        output_mask = output_file_name+"/iter*"
        command = f"hadoop fs -rm -r {output_mask}"
        subprocess.run(command, shell=True)