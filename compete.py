import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mr3px.csvprotocol import CsvProtocol
import csv
from math import radians, cos, sin, asin, sqrt
###############################################################
# python3 competition.py --file restaurant.csv restaurant.csv
# python3 compete.py --file sample_db.csv sample_db.csv > d10.csv
###############################################################

#df = pd.read_csv('sample.csv', sep=",")

class MRPair(MRJob):

    #OUTPUT_PROTOCOL = CsvProtocol

    def mapper_init(self):
        self.df = df 
        self.num = 0

    def mapper(self, _, line):

        line = np.array(line.split(','))
        self.num += 1
        #print(self.num)
        id1, lat1, lng1 = line[2], line[7], line[8]
        for i, row in self.df.iterrows():
            id2, lat2, lng2 = row[2], row[7], row[8]
            #print(lat2)
            if id2 != id1 and id2 != None:
                #yield (id1, id2), (lat1, lng1, lat2, lng2)
                l = [id1, id2]
                l.sort()
                if l[0] == id1: 
                    yield (id1, id2), (lat1, lng1, lat2, lng2)
                else:
                    yield (id2, id1), (lat1, lng1, lat2, lng2)
    '''  
    def combiner(self, key, value):
        location = list(value)

        lat1, lng1= location[0], location[1]
        lat2, lng2= location[2], location[3]
        #print(lat2)
        haversine = haversine(lat1, lng1, lat2, lng2)
        #levenshtein = self.levenshtein(cat1, cat2)
        #overlap_cat = self.category(cat1, cat2)
        yield key, haversine
        #except:
        #    yield None, None
    def reducer(self, key, haversine):
        haversine = list(haversine) 
        if haversine != None:
            print(haversine)
            if haversine[0] <= 10:     
                yield key[0], key[1]

    def reducer_final(self, key, value):
        yield ([key]+list(value), None) 
    '''

############################## auxiliary functions ##########################      
def haversine(lat1, lng1, lat2, lng2):
    '''
    Calculate the circle distance between two points
    on the earth (specified in decimal degrees)
    '''
    # convert decimal degrees to radians
    lng1, lat1, lng2, lat2 = radians(lng1), radians(lat1), radians(lng2), radians(lat2)

    # haversine formula
    dlng = lng2 - lng1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlng / 2)**2
    c = 2 * asin(sqrt(a))

    # 6367 km is the radius of the Earth
    km = 6367 * c
    m = km * 1000
    return m

    def category(self, cat_list1, cat_list2):
        '''
        Return True if there is at least one same category 
        for the two restaurant 
        '''
        for i in cat_list1:
            if i in cat_list2 and i != "Restaurants":
                return True 
            else:
                return False 

    def levenshtein(self, cat_list1, cat_list2):
        '''
        Calculates the Levenshtein distance between two categories.
        '''
        #cat_list1 = cat_list1.remove("Restaurants")
        #cat_list2 = cat_list2.remove("Restaurants")
        n, m = len(cat_list1), len(cat_list2)
        if n > m:
            # Make sure n <= m, to use O(min(n,m)) space
            cat_list1, cat_list2 = cat_list2, cat_list1
            n,m = m,n
        current = range(n+1)
        for i in range(1,m+1):
            previous, current = current, [i]+[0]*n
            for j in range(1,n+1):
                add, delete = previous[j]+1, current[j-1]+1
                change = previous[j-1]
                if cat_list1[j-1] != cat_list2[i-1]:
                    change = change + 1
                current[j] = min(add, delete, change)
            
        return current[n]

    def compute_stars_sim(rating1, rating2):
       '''
       Compute the star rating similarity between two businesses
       '''
       MAX_STARS = 5
       return MAX_STARS - abs(rating1 - rating2)

    def compute_price_range(range1, range2):
        MAX_PRICE = 4
        return MAX_PRICE - abs(range1 - range2)
##########################################################################


if __name__ == '__main__':

    df = pd.read_csv('sample_db.csv', sep=",")
    MRPair.run()