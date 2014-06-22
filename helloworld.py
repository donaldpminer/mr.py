import mr
import rpyc
import glob
import os.path as path

inputs = [ path.abspath(f) for f in glob.glob('small_testdata/*') ]
print 'inputs =', inputs

def mapf(k, v, params):
    for tok in v.strip().split():
        yield (tok.lower(), 1)

conn = rpyc.connect('localhost', 52485)

for i in inputs:
    conn.root.map('b', snakes._serialize_function(snakes.localfile_linereader), snakes._serialize_function(mapf), snakes._serialize_function(snakes.identity_reducer), snakes._serialize_function(snakes.hash_partitioner), snakes._serialize_function(snakes.stdout_kv_output), 2, {'inputfilepath' : i})


