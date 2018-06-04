import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
import csv
###########################################################################################
# python3 price_star_review.py --file price_star_review.csv <neighborhood pairs file name>
###########################################################################################

class MRAverage(MRJob):

    def mapper_init(self):
        self.df = pd.read_csv('price_star_review.csv', sep=",")

    def mapper(self, _, line):
        line = np.array(line.split(','))    
        id1, id2 = line[0], line[1]
        self.df = self.df.fillna(0)
        price2 = float(self.df[self.df['business_id'] == id2]['attributes.RestaurantsPriceRange2'])
        rating2 = float(self.df[self.df['business_id'] == id2]['stars'])
        num_review2 = float(self.df[self.df['business_id'] == id2]['review_count'])
        # min price, star, and review_count in original data are all not 0
        if price2 != 0 and rating2 != 0 and num_review2 != 0:       
            yield id1, (price2, rating2, num_review2)

    def reducer(self, key, value):
        sum_price = 0
        sum_rating = 0
        sum_review = 0
        count_n = 0
        count_p = 0
        count_r = 0 
        for i in value:
            if not i[0]:
                count_p += 0
            else:
                sum_price+= i[0]
                count_p += 1
            if not i[1]:
                count_r += 0 
            else:
                sum_rating += i[1]
                count_r += 1
            if not i[2]:
                count_n += 0
            else:
                sum_review += i[2]
                count_n += 1

        if count_p == 0:
            avg_p = 0
        elif count_r == 0:
            avg_r = 0
        elif count_n == 0:
            avg_n = 0
        else:
            avg_r = sum_rating/count_r
            avg_p = sum_price/count_p
            avg_n = sum_review/count_n

        yield key, (avg_p, avg_r, avg_n)


if __name__ == '__main__':
    MRAverage.run()