import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
import csv
###############################################################################
# python3 price.py --jobconf mapreduce.job.reduces=1 new_neighbor_500_6.txt.csv
###############################################################################

class MRAverage(MRJob):

    def mapper_init(self):
        self.df = pd.read_csv('price_star.csv', sep=",")

    def mapper(self, _, line):
        line = np.array(line.split(','))    
        id1, id2 = line[0], line[1]
        price2 = float(self.df[self.df['business_id'] == id2]['attributes.RestaurantsPriceRange2'])
        rating2 = float(self.df[self.df['business_id'] == id2]['stars'])
        yield id1, (price2, rating2)
    
    def reducer(self, key, value):
        sum_price = 0
        sum_rating = 0
        count_p = 0
        count_r = 0 
        for i in value:
            if not value[0]:
                count_p += 0
            else:
                sum_price+= i[0]
                count_p += 1
            if not value[1]:
                count_r += 0 
            else:
                sum_rating += i[1]
                count_r += 1
        if count_p != 0 and count_r != 0:
            yield key, (sum_price/count_p, sum_rating/count_r) 


if __name__ == '__main__':
    MRAverage.run()