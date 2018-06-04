import numpy as np
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
import csv
from gensim.corpora import Dictionary
from gensim.matutils import cossim
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
import re
##################################################################
# python3 sim.py --file CITY_rvw.csv --file whole_dict.txt new_neighbor_CITY.csv > sim_CITY.csv

# Calculate review similarity score of each pair restaurants,
# and then calculate average review similarity score of each neighborhood
##################################################################


class MRPair(MRJob):
    OUTPUT_PROTOCOL = protocol.TextProtocol


    def mapper_sim(self, _, line):
        line = np.array(line.split(','))
        try:
            id1, id2 = line[0].strip('"')[2:-1], line[1].strip('"')[2:-1]
            yield id1, id2
        except:
            pass

    def reducer_init_sim(self):
        self.dct = Dictionary.load_from_text('whole_dict.txt')
        self.df = pd.read_csv('MAK_rvw.csv')

    def reducer_sim(self, center, nbr):
        nbr_list = list(nbr)
        c = str(center)
        try:
            rvws1 = self.df[self.df['business_id']==c]['text'].tolist()[0]
        except:
            rvws1 = ""
        rvws1 = rvws_to_wordlist(rvws1, True)


        for n in nbr_list:
            try:
                rvws2 = self.df[self.df['business_id']==n]['text'].tolist()[0]
            except:
                rvws2 = ""
            rvws2 = rvws_to_wordlist(rvws2, True)

            sim = cossim(self.dct.doc2bow(rvws1), self.dct.doc2bow(rvws2))

            # yield str(c) + '\t' + str(n), str(sim)
            yield c, sim

    def reducer_average_sim(self, center, sim):
        sim_list = list(sim)
        avg = sum(sim_list)/len(sim_list)
        yield center, str(avg)

    def steps(self):
        return [
        MRStep(mapper=self.mapper_sim,
             reducer_init=self.reducer_init_sim,
             reducer=self.reducer_sim),
        MRStep(reducer=self.reducer_average_sim)
        ]


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


if __name__ == '__main__':
    # df = pd.read_csv('rvw_groupby_rest_little.csv')
    # print('df')
    # df = pd.read_csv('rvw_groupby_rest.csv')
    MRPair.run()
