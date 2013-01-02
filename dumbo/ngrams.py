import os
import re

class NgramMapper(object):
    
    def __init__(self):
        # determine value of n in the current block of ngrams
        input_file = os.environ['map_input_file']
        self.expected_tokens = int(re.findall(r'([\d]+)gram', os.path.basename(input_file))[0])
    
    def __call__(self, key, value):
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
    import dumbo
    # import pdb
    # pdb.set_trace()
    # dumbo.run(NgramMapper, reducer, combiner=combiner)
    dumbo.run(NgramMapper, reducer)