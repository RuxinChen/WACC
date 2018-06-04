import numpy as np
import unicodedata
from mrjob.job import MRJob


class MRDataCity(MRJob):

    def mapper(self, _, line):

        line = np.array(line.split('|'))
        city = line[83]
        yield city, 1 

    def combiner(self, city, counts):
        yield city, sum(counts)
  
    def reducer(self, city, counts):
    	total = sum(counts)
    	if total >= 2000:
    		yield city, total


if __name__ == '__main__':

    MRDataCity.run()