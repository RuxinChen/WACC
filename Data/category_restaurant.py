import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

# def rst_category(rst_city):
#     rst['categories'] = rst['categories'].apply(\
#         lambda x: x.strip('[').strip(']'))
#     rst['categories'] = rst['categories'].apply(lambda x: x.split(','))

class MRCategorize(MRJob):
    def mapper(self, _, line):
        reader = csv.reader([line])
        l = next(reader)
        bid = l[3] # Business ID
        cate = l[34].strip('[').strip(']') # Categories
        cate = cate.split(',')

        yield bid, cate


    def combiner(self, bid, cate):
        cate_ListOfList = list(cate)
        for cate_list in cate_ListOfList:
            for c in cate_list:
                yield c, bid


    def reducer(self, c, bid):

        yield c, list(bid)



if __name__ == '__main__':
    MRCategorize.run()
# Import csv file: cs123_restaurants.csv
# python3 category_restaurant.py  --jobconf mapreduce.jobreduces=1 restaurant.csv
