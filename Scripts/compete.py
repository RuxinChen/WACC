import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
import csv
from math import radians, cos, sin, asin, sqrt
########################################################################
# python3 compete.py --file neighbor.csv neighbor.csv > neighbor_d3.csv 
########################################################################

class MRPair(MRJob):
    '''
    The class yield the restaurant pairs with its haversine distance
    '''

    OUTPUT_PROTOCOL = protocol.TextProtocol

    def mapper_init(self):
        self.df = pd.read_csv('neighbor.csv', sep=",", header = None)
        self.lst_of_city = ["Las Vegas", "Phoenix", "Toronto", "Charlotte"]

    def mapper(self, _, line):

        line = next(csv.reader([line]))
        id1, lat1, lng1, city1= line[0], line[1], line[2], line[3][2:][:-1]
        for i, row in self.df.iterrows():
            id2, lat2, lng2, city2 = row[0], row[1], row[2], row[3][2:][:-1]
            if city1 in self.lst_of_city:
                if id2 != id1 and id2 != None and city1 == city2:
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
        h = haversine(lat1, lng1, lat2, lng2)
        yield key, h

    def reducer(self, key, haversine):
        haversine = list(haversine) 
        if haversine != None:
            if haversine[0] <= 3:     
                yield key[0]+'\t'+key[1], str(haversine[0])
    

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

##########################################################################

if __name__ == '__main__':

    MRPair.run()