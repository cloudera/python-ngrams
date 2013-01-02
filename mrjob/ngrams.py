#! /usr/bin/env python

import os
import re

from mrjob.job import MRJob
from mrjob.protocol import RawProtocol, ReprProtocol

class NgramNeighbors(MRJob):
    
    # mrjob allows you to specify input/intermediate/output serialization
    # default output protocol is JSON; here we set it to text
    OUTPUT_PROTOCOL = RawProtocol
    
    def mapper_init(self):
        # determine value of n in the current block of ngrams by parsing filename
        input_file = os.environ['map_input_file']
        self.expected_tokens = int(re.findall(r'([\d]+)gram', os.path.basename(input_file))[0])
    
    def mapper(self, key, line):
        data = line.split('\t')
        
        # error checking
        if len(data) < 3:
            return
        
        # unpack data
        ngram = data[0].split()
        year = data[1]
        count = int(data[2])
        
        # more error checking
        if len(ngram) != self.expected_tokens:
            return
        
        # generate key
        pair = sorted([ngram[0], ngram[self.expected_tokens - 1]])
        k = pair + [year]
        
        # note that the key is an object (a list in this case)
        # that mrjob will serialize as JSON text
        yield (k, count)
    
    def combiner(self, key, counts):
        # the combiner must be separate from the reducer because the input
        # and output must both be JSON
        yield (key, sum(counts))
    
    def reducer(self, key, counts):
        # the final output is encoded as text
        yield "%s\t%s\t%s" % tuple(key), str(sum(counts))

if __name__ == '__main__':
    # sets up a runner, based on command line options
    NgramNeighbors.run()
