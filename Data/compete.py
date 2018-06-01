import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mr3px.csvprotocol import CsvProtocol
import csv
from math import radians, cos, sin, asin, sqrt
########################################################################
# python3 competition.py --file restaurant.csv restaurant.csv
# python3 compete.py --file neighbor.csv neighbor.csv > neighbor_d10.csv
########################################################################

#df = pd.read_csv('sample.csv', sep=",")

class MRPair(MRJob):

    #OUTPUT_PROTOCOL = CsvProtocol

    def mapper_init(self):
        self.df = df 
        self.lst_of_city = lst_of_city

    def mapper(self, _, line):
        line = np.array(line.split(','))
        #self.num += 1
        #print(self.num)
        #line = next(csv.reader([line]))
        #print(line)
        id1, lat1, lng1, city1= line[0], line[1], line[2], line[3][2:][:-1]
        for i, row in self.df.iterrows():
            id2, lat2, lng2, city2 = row[0], row[1], row[2], row[3][2:][:-1]
            #print(id2)
            #print(lat2)
            print(city1)
            if city1 in self.lst_of_city:
                print(city1)
                if id2 != id1 and id2 != None and city1 == city2:
                    #yield (id1, id2), (lat1, lng1, lat2, lng2)
                    l = [id1, id2]
                    l.sort()
                    if l[0] == id1: 
                        yield (id1, id2), (lat1, lng1, lat2, lng2)
                    else:
                        yield (id2, id1), (lat2, lng2, lat1, lng1)

    def combiner(self, key, value):
        location = list(value)[0]
        lat1, lng1= float(location[0]), float(location[1])
        lat2, lng2= float(location[2]), float(location[3])
        #print(lat2)
        h = haversine(lat1, lng1, lat2, lng2)
        #levenshtein = self.levenshtein(cat1, cat2)
        #overlap_cat = self.category(cat1, cat2)
        yield key, h
        #except:
        #    yield None, None

    def reducer(self, key, haversine):
        haversine = list(haversine) 
        if haversine != None:
            if haversine[0] <= 1000:     
                yield key[0], key[1]

    def reducer_final(self, key, value):
        lst = list(value)
        yield key, lst
        #yield (None, [key]+lst) 

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
            mapper = self.mapper,
            combiner=self.combiner,
            reducer=self.reducer),
        MRStep(reducer=self.reducer_final)]
    

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
    return km

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

    df = pd.read_csv('neighbor.csv', sep=",", header = None)
    lst_of_city = ["Las Vegas", "Phoenix", "Toronto", "Montreal"]
    MRPair.run()