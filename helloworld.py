import mr

try:
    mr.delete('LICENSE')
except OSError as ose:
    print 'tried to delete license but it wasnt there', ose

try:
    mr.delete('README')
except OSError as ose:
    print 'tried to delete readme but it wasnt there', ose

try:
    mr.rmdir('helloout/')
except OSError as ose:
    print 'tried to delete helloout/ but i couldnt', ose

mr.put('small_testdata/LICENSE.txt', 'LICENSE')
mr.put('small_testdata/README.txt', 'README')

def mapf(k, v, params):
    for tok in v.strip().split():
        yield (tok.lower(), 1)


conn = mr._connect(mr.random_slave())

sf = mr._serialize_function

print 'premap', mr.ls()

conn.root.map(sf(mr.dfs_linereader), sf(mapf), sf(mr.identity_reducer), sf(mr.hash_partitioner), sf(mr.dfs_linewriter), 2, \
{'inputfilepath' : 'LICENSE', 'outputdir' : 'helloout'})


conn.root.map(sf(mr.dfs_linereader), sf(mapf), sf(mr.identity_reducer), sf(mr.hash_partitioner), sf(mr.dfs_linewriter), 2, \
{'inputfilepath' : 'README', 'outputdir' : 'helloout'})


print 'postmap', mr.ls()



for k in mr.basic_sort(mr.basic_shuffle(1, {'outputdir':'helloout'}), {}):
    print k
