#! /usr/bin/env python

import sys

total = 0
prev_key = False
for line in sys.stdin:
    data = line.split('\t')
    curr_key = '\t'.join(data[:3])
    count = int(data[3])
    
    # found a boundary; emit current sum
    if prev_key and curr_key != prev_key:
        print >>sys.stdout, "%s\t%i" % (prev_key, total)
        prev_key = curr_key
        total = count
    # same key; accumulate sum
    else:
        prev_key = curr_key
        total += count

# emit last key
if prev_key:
    print >>sys.stdout, "%s\t%i" % (prev_key, total)
