import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
# from mr3px.csvprotocol import CsvProtocol
import csv
from math import radians, cos, sin, asin, sqrt
from gensim.corpora import Dictionary
from gensim.matutils import cossim
from nltk.corpus import stopwords
import re
################################################################################
# python3 rvw_sim.py --file n_sample5_2.csv n_samle5_2.csv > rvw_sim.txt
############################################################################

class MRPair(MRJob):

    # OUTPUT_PROTOCOL = CsvProtocol
    OUTPUT_PROTOCOL = protocol.TextProtocol

    def mapper_init(self):
        # location = r"/Users/mengchenshi/Downloads/Spr-18/CS/Project/Codes2/rvw_groupby_rest_little.csv"
        self.df = pd.read_csv('rvw_groupby_rest_little.csv')
        # self.df = pd.read_csv('rvw_groupby_rest_little.csv')


    def mapper(self, _, line):
        line = np.array(line.split(','))
        id1, id2 = line[0].strip('"')[2:-1], line[1].strip('"')[2:-1]
        # print(id1)

        try:
            rvws1 = self.df[self.df['business_id']==id1]['text'][0]
        except:
         # IndexError:
            rvws1 = ''
        # print('rvws1: ',rvws1)


        try:
            rvws2 = self.df[self.df['business_id']==id2]['text'][0]
        except:
         # IndexError:
            rvws2 = ''
        # print('rvws2: ',rvws2)

        rvws1 = rvws_to_wordlist(rvws1, True)
        rvws2 = rvws_to_wordlist(rvws2, True)
        # print('mapper')

        yield (id1, id2), (rvws1, rvws2)

    def reducer_init(self):
        # location = r"/Users/mengchenshi/Downloads/Spr-18/CS/Project/Codes2/rvw_groupby_rest_little.csv"
        self.df = pd.read_csv('rvw_groupby_rest_little.csv')
        # self.df = pd.read_csv('rvw_groupby_rest_little.csv')

        self.rvws = self.df['text'].tolist()
        self.rvws = [rvws_to_wordlist(r, True) for r in self.rvws]
        self.dct = Dictionary(self.rvws)
        # print(self.dct)

    def reducer(self, key, value):
        lst_of_rvws = list(value)[0]
        rvws1 = lst_of_rvws[0]
        rvws2 = lst_of_rvws[1]
        # print(rvws1)
        sim = cossim(self.dct.doc2bow(rvws1), self.dct.doc2bow(rvws2))
        # similarity.append((biz[i], biz[j], sim))
        # print('reducer')
        join_key = str(key[0]) + '\t' + str(key[1])
        yield join_key, str(sim)



############################## auxiliary functions ##########################

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
        stops = set(stopwords.words("english"))
        words = [w for w in words if not w in stops]
    return words


##########################################################################

if __name__ == '__main__':
    # df = pd.read_csv('rvw_groupby_rest_little.csv')
    # print('df')
    # df = pd.read_csv('rvw_groupby_rest.csv')
    MRPair.run()
