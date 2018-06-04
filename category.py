import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
import csv
from math import radians, cos, sin, asin, sqrt
##################################################################################
# python3 compete.py --file new_neighbor_d3.csv -r dataproc --num-core-instances 8
# new_neighbor_d3.csv > category_d3.csv
##################################################################################

class MRPair(MRJob):

    OUTPUT_PROTOCOL = protocol.TextProtocol

    def mapper_init(self):
        self.df = pd.read_csv('category.csv', sep=",", encoding='utf-8')

    def mapper(self, _, line):

        line = np.array(line.split(','))
        id1, id2 = line[0], line[1]
        cat1 = list(self.df[self.df['business_id'] == id1]['categories'])
        cat1 = cat1[0].split(",")
        cat1[0] = cat1[0][2:-1]
        cat1[-1] = cat1[-1][2:-1]
        cat2 = list(self.df[self.df['business_id']== id2]['categories'])
        cat2 = cat2[0].split(",")
        cat2[0] = cat2[0][2:-1]
        cat2[-1] = cat2[-1][2:-1]
        yield (id1, id2), (cat1, cat2)

    def reducer(self, key, value):
        lst_of_categories = list(value)[0]
        cat1 = lst_of_categories[0]
        cat2 = lst_of_categories[1]
        if "Restaurants" in cat1:
            cat1.remove("Restaurants")
        if "Restaurants" in cat2:
            cat2.remove("Restaurants")
        l = levenshtein(cat1, cat2)
        o = category(cat1, cat2)
        yield key[0]+'\t'+ key[1], str(l) + '\t' + str(o)


############################## auxiliary functions ##########################      

def category(cat_list1, cat_list2):
    '''
    Return True if there is at least one same category 
    for the two restaurant 
    '''
    for i in cat_list1:
        if i in cat_list2 and i != "Restaurants":
            return True 
        else:
            return False 

def levenshtein(cat_list1, cat_list2):
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

##########################################################################

if __name__ == '__main__':

    MRPair.run()