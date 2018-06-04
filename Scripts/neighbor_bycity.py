import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
from math import radians, cos, sin, asin, sqrt
import csv
from gensim.corpora import Dictionary
from gensim.matutils import cossim
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS

import re
########################################################################
# This file get restaurant pairs (two restaurants less than 1.5kms from each
# other) in the given cities. All resturants that paired with one resturant
# compose a "neighborhood"


# python3 compete_bycity_2col.py neighbor.csv > neighbor_CITY.csv
########################################################################




class MRPair(MRJob):

    OUTPUT_PROTOCOL = protocol.TextProtocol

    def mapper_init(self):
        self.lst_of_city = ["Phoenix", "Cleveland", "Madison", "Markham"]


    def mapper(self, _, line):
        line = np.array(line.split(','))

        id1, lat, lng, city = line[0], line[1], line[2], line[3][2:][:-1]
        if city in self.lst_of_city:
            yield city, (id1, lat, lng)

    def reducer(self, city, values):
        values = list(values)
        for v1 in values:
            for v2 in values:
                if v1[0] != v2[0]:
                    lat1, lng1 = float(v1[1]), float(v1[2])
                    lat2, lng2 = float(v2[1]), float(v2[2])
                    h = haversine(lat1, lng1, lat2, lng2)
                    if h <= 1.5: # distance less than 1.5km
                        # yield 2 business_id columns, city and distance
                        # between the 2 restaurants
                        yield v1[0] + '\t' + v2[0] + '\t' + city, str(h)




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
