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
    conn.root.map('b', mr._serialize_function(mr.localfile_linereader), mr._serialize_function(mapf), mr._serialize_function(mr.identity_reducer), mr._serialize_function(mr.hash_partitioner), mr._serialize_function(mr.stdout_kv_output), 2, {'inputfilepath' : i})


