import pandas as pd 
import numpy as np 
import csv

def merge():

    with open("merged.csv", 'w') as file:
        writer = csv.writer(file, delimiter=',')
        for j in lst:
            df = pd.read_csv(j, sep='\t')
            for i in range(df.shape[0]):
                row = list(df.iloc[i, :])[1][1:-1]
                row = row.split(",")
                writer.writerow(row)


if __name__ == '__main__':

    lst =['toronto_kmeans.csv','las_vegas_kmeans.csv', \
    'phoenix_kmeans.csv','charlotte_kmeans.csv', \
    'pittsburgh_kmeans.csv', 'edinburgh_kmeans.csv', \
    'cleveland_kmeans.csv', 'mississauga_kmeans.csv', \
    'mesa_kmeans.csv', 'stuttgart_kmeans.csv', \
    'madison_kmeans.csv', 'markham_kmeans.csv']

    merge()