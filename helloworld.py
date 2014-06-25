import mr

try:
    mr.delete('LICENSE')
except OSError as ose:
    print 'tried to delete license but it wasnt there'

mr.put('small_testdata/LICENSE.txt', 'LICENSE')

def mapf(k, v, params):
    for tok in v.strip().split():
        yield (tok.lower(), 1)


conn = mr._connect(mr.random_slave())

sf = mr._serialize_function

conn.root.map(sf(mr.dfs_linereader), sf(mapf), sf(mr.identity_reducer), sf(mr.hash_partitioner), sf(mr.dfs_linewriter), 2, \
{'inputfilepath' : 'LICENSE', 'outputdir' : 'helloout'})

