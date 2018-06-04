import numpy as np
import pandas as pd
import csv
from gensim.corpora import Dictionary
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
import re

#######################################################################
# Train gensim Dictionary model using all reviews fo all restaurants

#######################################################################

def go():
    df = pd.read_csv('rvw_groupby_rest.csv')

    rvws = df['text'].tolist()
    rvws = [rvws_to_wordlist(r, True) for r in rvws]
    dct = Dictionary(rvws)
    print(type(dct))
    dct.save_as_text('whole_dict.txt')


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
        words = [w for w in words if not w in ENGLISH_STOP_WORDS]
    return words

if __name__ == '__main__':
    go.run()
