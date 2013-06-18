import os
import re

import luigi
import luigi.hadoop
import luigi.hdfs

class InputText(luigi.ExternalTask):
    path = luigi.Parameter()
    
    def output(self):
        return luigi.hdfs.HdfsTarget(self.path)

class Ngrams(luigi.hadoop.JobTask):
    source = luigi.Parameter()
    destination = luigi.Parameter()
    # overrides superclass; gets set as jobconf:
    n_reduce_tasks = luigi.IntParameter(default=10)
    
    def requires(self):
        tasks = []
        paths = luigi.hdfs.HdfsClient().listdir(self.source, ignore_directories=True, recursive=True)
        for path in paths:
            tasks.append(InputText(path))
        return tasks
    
    def output(self):
        return luigi.hdfs.HdfsTarget(self.destination)
    
    def init_mapper(self):
        input_file = os.environ['map_input_file']
        self.expected_tokens = int(re.findall(r'([\d]+)gram', os.path.basename(input_file))[0])
    
    def mapper(self, line):
        data = line.split('\t')
        
        if len(data) < 3:
            return
        
        # unpack data
        ngram = data[0].split()
        year = data[1]
        count = int(data[2])
        
        if len(ngram) != self.expected_tokens:
            return
        
        # generate key
        pair = sorted([ngram[0], ngram[self.expected_tokens - 1]])
        k = pair + [year]
        
        yield (k, count)
    
    def combiner(self, key, values):
        yield (key, sum(values))
    
    def reducer(self, key, values):
        yield "%s\t%s\t%s" % tuple(key), str(sum(values))
    
if __name__ == '__main__':
    luigi.run()
