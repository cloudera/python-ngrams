from hadoopy import launch

input_path = 'hdfs://laserson-1.ent.cloudera.com/ngrams'
output_path = 'hdfs://laserson-1.ent.cloudera.com/output-hadoopy'

launch(input_path,
       output_path,
       'ngrams.py',
       use_seqoutput=False,
       num_reducers=10,
       hstreaming='/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.1.2.jar')
