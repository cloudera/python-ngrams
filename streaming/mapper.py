#! /usr/bin/env python

import os
import re
import sys

# determine value of n in the current block of ngrams by parsing the filename
input_file = os.environ['map_input_file']
expected_tokens = int(re.findall(r'([\d]+)gram', os.path.basename(input_file))[0])

for line in sys.stdin:
    data = line.split('\t')
    
    # perform some error checking
    if len(data) < 3:
        continue
    
    # unpack data
    ngram = data[0].split()
    year = data[1]
    count = data[2]
    
    # more error checking
    if len(ngram) != expected_tokens:
        continue
    
    # build key and emit
    pair = sorted([ngram[0], ngram[expected_tokens - 1]])
    print >>sys.stdout, "%s\t%s\t%s\t%s" % (pair[0], pair[1], year, count)
