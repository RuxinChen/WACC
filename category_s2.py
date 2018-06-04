import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
import csv
from math import radians, cos, sin, asin, sqrt
##################################################################################
# python3 category_s2.py new_category_d3.csv > category_d3.csv
##################################################################################

class MRCat(MRJob):

	OUTPUT_PROTOCOL = protocol.TextProtocol

    def mapper(self, _, line):

    	line = np.array(line.split(','))
    	bid1 = line[0]
    	bid2 = line[1]
    	l = line[2]
    	o = line[3]
    	yield (bid1, bid2), (l, o)

    def reducer(self, key, value):
    	value = list(value)
    	l = [value[i][0] for i in range(len(value))]
    	o = [value[i][1] for i in range(len(value))]
    	avg_l = sum(l)/len(l)
    	avg_o = sum(o)/len(o)
    	yield key[0]+'\t'+ key[1], str(avg_l) + '\t' + str(avg_o)

##########################################################################

if __name__ == '__main__':

    MRCat.run()

