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

    #OUTPUT_PROTOCOL = protocol.TextProtocol

    def mapper_init(self):
        self.df = pd.read_csv('price_star.csv', sep=",")
        #self.df = pd.read_csv('pricestar.csv', sep=",")

    def mapper(self, _, line):
        line = np.array(line.split(','))    
        id1, id2 = line[0], line[1]
        #price2 = line[6]
        #price1 = float(self.df[self.df['business_id'] == id1]['attributes.RestaurantsPriceRange2'])
        price2 = float(self.df[self.df['business_id'] == id2]['attributes.RestaurantsPriceRange2'])
        if price2 is not None:
            yield id1, price2
    
    def reducer(self, key, value):
        sum_price = 0
        count = 0
        for i in value:
            sum_price+= i
            count += 1
        if count != 0:
            yield key, sum_price/count


if __name__ == '__main__':
    MRAverage.run()