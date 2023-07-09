import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def get_param(df_column):
    # Lista per salvare i valori estratti
    input_df = []

    for input_string in df_column:
        # Rimuovi i caratteri non numerici dalla stringa
        numeric_string = str(input_string)

        # Estrai il primo valore
        split1 = numeric_string.split('_')[1].split('x')
        split2 = numeric_string.split('_')[2]

        dim = int(split1[0])
        num_feat = int(split1[1])
        num_cluster = int(split2.split('.')[0])
        complexity = dim * num_feat * num_cluster
        input_df.append([dim, num_feat, num_cluster, complexity])
    return input_df


def get_x_compl_plot(x, name_col):
    # Trova i valori unici di 'tolerance'
    tolerance_values = df['tolerance'].unique()

    # Definisci i colori per i diversi valori di 'tolerance'
    color_map = {value: plt.get_cmap('Set1')(i) for i, value in enumerate(tolerance_values)}

    df_sorted = x.sort_values(['complexity'])
    df_sorted['group_num'] = df_sorted.groupby(['complexity', 'tolerance']).ngroup()
    df_sorted = df_sorted.sort_values(['group_num'])

    print(df_sorted)

    # Crea il grafico a bolle (bubble chart) per il singolo DataFrame
    plt.scatter(df_sorted['group_num'], df_sorted[name_col],
                c=df_sorted['tolerance'].map(color_map), alpha=0.6)

    # Unisci i punti con una linea utilizzando i colori della tolerance
    for tolerance, color in color_map.items():
        # Filtra i dati corrispondenti alla tolerance specifica
        df_group = df_sorted[df_sorted['tolerance'] == tolerance]
        # Unisci i punti con una linea utilizzando i dati filtrati
        plt.plot(df_group['group_num'], df_group[name_col], color=color, linewidth=1)

    # Aggiungi etichette agli assi
    plt.xlabel('Complexity')
    plt.ylabel(name_col)
    # Mostra la legenda dei colori
    for value, color in color_map.items():
        plt.scatter([], [], color=color, label=value)
    plt.legend().set_title('Tolerance')
    # Aggiungi griglia
    plt.grid(True)

    # Imposta lo sfondo trasparente
    plt.figure(facecolor='none')
    # Mostra i grafici per i diversi 'numReducers'
    plt.show()


def get_bar_plot(x):
    x['ratio'] = x['executionTime'] / x['numReducers']
    # Creazione del barplot
    plt.bar(x['numReducers'], x['ratio'])

    # Aggiunta delle etichette agli assi
    plt.xlabel('numReducers')
    plt.ylabel('Ratio')

    # Mostra il grafico
    plt.show()


# Carica il dataset
df = pd.read_csv('results_PKMeans.csv')

# Lista per salvare i valori estratti
input_df = get_param(df.inputPath)
print(input_df)
print(df.columns)

params_columns = ['dim', 'num_feat', 'num_cluster', 'complexity']
params_df = pd.DataFrame(input_df, columns=params_columns)

# Aggiungi le nuove colonne al DataFrame originale
df = pd.concat([df, params_df], axis=1)
df = df.drop('k', axis=1)

# Trova i valori unici di 'numReducers'
num_reducers_values = df['numReducers'].unique()

# Suddividi il DataFrame in base al valore di 'numReducers'
dfs_for_reducer = []
for reducer in num_reducers_values:
    df_reducer = df[df['numReducers'] == reducer]
    dfs_for_reducer.append(df_reducer)

get_x_compl_plot(dfs_for_reducer[0], "iter")
get_x_compl_plot(dfs_for_reducer[1], "iter")

get_x_compl_plot(dfs_for_reducer[0], "executionTime")
get_x_compl_plot(dfs_for_reducer[1], "executionTime")

get_bar_plot(df)
