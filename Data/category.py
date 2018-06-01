import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mr3px.csvprotocol import CsvProtocol
import csv
from math import radians, cos, sin, asin, sqrt
################################################################################
# python3 compete.py --file neighbor_d3.csv neighbor_d3.csv > category_d3.csv
################################################################################

class MRPair(MRJob):

    OUTPUT_PROTOCOL = CsvProtocol

    def mapper_init(self):
        self.df = df 
        self.lst_of_city = lst_of_city

    def mapper(self, _, line):
        line = np.array(line.split(','))
        id1, cat1 = line[0], line[1]
        for i, row in self.df.iterrows():
            id2, cat2 = row[0], row[1]
            if city1 in self.lst_of_city:
                if id2 != id1 and id2 != None:
                    l = [id1, id2]
                    l.sort()
                    if l[0] == id1: 
                        yield (id1, id2), (cat1, "||", cat2)
                    else:
                        yield (id2, id1), (cat2, "||", cat1)

    def reducer(self, key, value):
        lst_of_categories = list(value)[0]
        if "Restaurants" in lst_of_categories:
            lst_of_categories.remove("Restaurants")
        split = lst_of_categories.index("||")
        cat1 = lst_of_categories[:split]
        cat2 = lst_of_categories[split:]
        levenshtein = self.levenshtein(cat1, cat2)
        overlap_cat = self.category(cat1, cat2)
        yield (None, [key, levenshtein, overlap_cat])
    

############################## auxiliary functions ##########################      

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

##########################################################################

if __name__ == '__main__':

    df = pd.read_csv('neighbor_d3.csv', sep=",", header = None)
    lst_of_city = ["Las Vegas", "Phoenix", "Toronto", "Montreal"]
    MRPair.run()