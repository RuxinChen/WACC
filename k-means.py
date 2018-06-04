from mrjob.job import MRJob
import pandas as pd 
from math import sin, cos, sqrt, atan2, radians
import sys
from sklearn.cluster import KMeans
from mrjob.step import MRStep
import numpy as np
import csv
import heapq
##################################################################################
# python3 k-means.py --jobconf mapreduce.job.reduces=1 --file cleveland.csv cleveland.csv> cleveland_kmeans.csv
##################################################################################

class MRkmeans(MRJob):

    def mapper_init(self):

        self.df = pd.read_csv("cleveland.csv", sep=",", names=['business_id', \
            'lat', 'lng', 'city', 'star','price', 'review'])
        self.df = self.df.sample(frac=0.2, replace=False)

        self.df['lat']= self.df['lat'].astype(float)
        self.df['lng']= self.df['lng'].astype(float)

        self.location = self.df[['lng', 'lat']]
        self.kmeans = KMeans(n_clusters= 10, random_state=1234).fit(np.array(self.location))

    def mapper(self, _, line):

        line = np.array(line.split(','))
        try: 
            lat, lng = float(line[1]), float(line[2])
            star = float(line[4])
            price = float(line[5])
            review = int(line[6])
            cluster = self.kmeans.predict(np.array([lng, lat]).reshape(1,-1))[0]
            key = int(cluster)
            yield key, (1,  star, price, review)
        except:
            key = None 

    def reducer(self, key, value):
        value = list(value)
        count = [value[i][0] for i in range(len(value))]
        star = [value[i][1] for i in range(len(value))]
        price = [value[i][2] for i in range(len(value))]
        review = [value[i][3] for i in range(len(value))]
        avg_star = sum(star)/len(star)
        avg_price = sum(price)/len(star)
        avg_review = sum(review)/len(star)
        yield key, (sum(count), avg_star, avg_price, avg_review)

    def reducer_init(self):
        # initialize the heap queue 
        self.h = [(0, None, None, None, None)]*10
        heapq.heapify(self.h)     

    def reducer_2(self, key, value):
        value = list(value)[0]
        count, avg_star, avg_price, avg_review = value[0], \
        value[1], value[2], value[3]
        min_count, min_key = self.h[0][0:2]
        if count > min_count:
            heapq.heapreplace(self.h, (count, [key, avg_star, avg_price, avg_review]))

    def reducer_final(self):
        self.h.sort(reverse=True)
        for tup in self.h:
            yield tup[1][0], (tup[0], tup[1][1], tup[1][2], tup[1][3])

    def steps(self):
        return [MRStep(
                       mapper_init = self.mapper_init,
                       mapper = self.mapper,
                       reducer=self.reducer),
        
                MRStep(reducer_init=self.reducer_init,
                    reducer = self.reducer_2,
                    reducer_final = self.reducer_final)
                ]

if __name__ == '__main__':

    MRkmeans.run()

