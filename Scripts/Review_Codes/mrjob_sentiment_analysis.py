import nltk
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from mrjob.job import MRJob
import re
import csv
import sys
import numpy as np
from mr3px.csvprotocol import CsvProtocol

csv.field_size_limit(sys.maxsize)

WORD_RE = re.compile(r"[\w]+")
###############################################################
#python3 mrjob_sentiment_analysis.py cleaned_review_rest.csv > sentiment_analysis.csv


# This file calculates the review sentiment of each restaurant
###############################################################

class MRSentAnalysis(MRJob):

    OUTPUT_PROTOCOL = CsvProtocol

    def mapper_init(self):
        self.sia = SentimentIntensityAnalyzer()

    def mapper(self, _, line):
        reader = csv.reader(line.splitlines())

        l = next(reader)
        bid = l[3]

        try:
            rvw = l[6]
        except:
            rvw = ''
        score = self.sia.polarity_scores(rvw)['compound']
        yield bid, score

    def reducer(self, bid, score):
        score_list = list(score)
        num_rvw = len(score_list)

        median_score = np.median(score_list)
        positive_pct = len([s for s in score_list if s > 0])/len(score_list)

        # yield number of reviews, median sentiment score of all reviews of a
        # restaurant, and the percentage of positive reviews
        yield (None, [bid] + [num_rvw, median_score, positive_pct])



if __name__ == '__main__':
    MRSentAnalysis.run()

