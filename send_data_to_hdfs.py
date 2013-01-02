import sys
import random
import subprocess

GB = 1024 ** 3
def du():
    p = subprocess.Popen('hadoop fs -du -s /ngrams', shell=True, stdout=subprocess.PIPE)
    return int(p.stdout.read().split()[0])

# generate list of URLs
base_url = 'http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-%igram-20090715-%i.csv.zip'
sizes = [(2, 100), (3, 200), (4, 400), (5, 800)]
ngram_urls = []
for size in sizes:
    n = size[0]
    num_files = size[1]
    for i in xrange(num_files):
        ngram_urls.append(base_url % (n, i))

# download data directly into HDFS
stream_cmd = 'curl "%s" | funzip | hadoop fs -put - /ngrams/%s'
random.shuffle(ngram_urls)
finished = False
while not finished:
    url = ngram_urls.pop()
    filename = '.'.join(url.split('/')[-1].split('.')[:-1])
    sys.stdout.write("%s\n" % filename)
    sys.stdout.flush()
    subprocess.Popen(stream_cmd % (url, filename), shell=True).wait()
    if du() > 20 * GB:
        finished = True
