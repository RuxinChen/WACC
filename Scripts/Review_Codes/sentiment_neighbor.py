import numpy as np
import pandas as pd
from mrjob import protocol
import csv
###############################################################################
#python3 sentiment_neighbor.py --file 'sentiment_analysis.csv' new_neighbor_CITY.csv > sent_CITY.csv

# Calculate average sentiment score of each neighborhood
###############################################################################

class MRAverage(MRJob):

    def mapper_init(self):
        self.df = pd.read_csv('sentiment_analysis.csv', sep=",")

    def mapper(self, _, line):
        line = np.array(line.split(','))
        id1, id2 = line[0][2:-1], line[1][2:-1]
        try:
            score2 = self.df[self.df['business_id']==id2]['score'].tolist()[0]
            yield id1, score2
        except IndexError:
            pass
        yield id1, id2



    def reducer(self, key, value):
        # score_list = list(value)
        sum_score = 0
        num_nbr = 0
        for score in score_list:
        # for score in [1,2,3,4,5]:
            if score:
                num_nbr += 1
                sum_score += score

        if num_nbr != 0:
            yield key, sum_score / num_nbr
        else:
            yield key, None


if __name__ == '__main__':
    MRAverage.run()
