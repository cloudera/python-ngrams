import os
import re

import hadoopy

class Mapper(object):
    
    def __init__(self):
        # determine value of n in the current block of ngrams
        input_file = os.environ['map_input_file']
        self.expected_tokens = int(re.findall(r'([\d]+)gram', os.path.basename(input_file))[0])
    
    def map(self, key, value):
        data = value.split('\t')
        
        if len(data) < 3:
            return
        
        ngram = data[0].split()
        year = data[1]
        count = int(data[2])
        
        if len(ngram) != self.expected_tokens:
            return
        
        pair = sorted([ngram[0], ngram[self.expected_tokens - 1]])
        k = pair + [year]
        
        yield (k, count)        

def combiner(key, values):
    yield (key, sum(values))

def reducer(key, values):
    yield "%s\t%s\t%s" % tuple(key), str(sum(values))

if __name__ == '__main__':
    hadoopy.run(Mapper, reducer, combiner)
