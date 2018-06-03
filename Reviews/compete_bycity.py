import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
#from mr3px.csvprotocol import CsvProtocol
from math import radians, cos, sin, asin, sqrt
import csv
from gensim.corpora import Dictionary
from gensim.matutils import cossim
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
# from nltk.corpus import stopwords
import re
########################################################################
# python3 compete_bycity.py --file small_dict.txt joint.csv
########################################################################

class MRPair(MRJob):

    #OUTPUT_PROTOCOL = CsvProtocol
    OUTPUT_PROTOCOL = protocol.TextProtocol

    # def configure_options(self):
    #     super(MRPair, self).configure_options()
    #     self.add_file_option('--items', help='path to neighbor.csv')

    # def mapper_init(self):
    #     self.df = pd.read_csv('neighbor.csv', sep=",", header = None)


    def mapper(self, _, line):
        line = np.array(line.split(','))
        # print(len(line))

        if len(line) > 6:
            id1, lat, lng, city, stars, price = line[0], line[1], line[2], line[3][2:][:-1], line[4], line[5]
            try:
                review = ' '.join(line[6:])
            except:
                review = ''

            review = rvws_to_wordlist(review, True)
            yield city, (id1, lat, lng, stars, price, review)

    def reducer_init(self):
        # self.df = pd.read_csv('rvw_groupby_rest_little.csv')
        # self.rvws = self.df['text'].tolist()
        # self.rvws = [rvws_to_wordlist(r, True) for r in self.rvws]
        # self.dct = Dictionary(self.rvws)
        self.dct = Dictionary.load_from_text('little_dict.txt')

    def reducer(self, city, values):
        values = list(values)
        for v1 in values:
            for v2 in values:
                if v1[0] != v2[0]:
                    lat1, lng1, review1 = float(v1[1]), float(v1[2]), v1[5]
                    lat2, lng2, review2 = float(v2[1]), float(v2[2]), v2[5]
                    h = haversine(lat1, lng1, lat2, lng2)
                    if h <= 3:
                        sim = cossim(self.dct.doc2bow(review1), self.dct.doc2bow(review2))
                        yield v1[0] + '\t' + v2[0]+ '\t' + str(city) + '\t' \
                        + str(v1[3]) + '\t' + str(v2[3]) + '\t' + str(v1[4])\
                         + '\t' + str(v2[4]) + '\t' + str(sim), str(h)


                        # yield v1[0]+'\t'+v2[0], str(h)

    # def reducer(self, city, value):
    #     for



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


def rvws_to_wordlist(rvws, remove_stopwords=False):
    '''
    Function to convert a document to a sequence of words,
    optionally removing stop words.  Returns a list of words.
    Input:
    review(string): one review
    Return:
    words(list of strings): words in the view with stopwords removed(optional)
    '''
    rvws_text = re.sub("[^a-zA-Z]", " ", rvws)
    words = rvws_text.lower().split()
    if remove_stopwords:
        # stops = set(stopwords.words("english"))
        # words = [w for w in words if not w in stops]
        words = [w for w in words if not w in ENGLISH_STOP_WORDS]
    return words
##########################################################################

if __name__ == '__main__':

    MRPair.run()
