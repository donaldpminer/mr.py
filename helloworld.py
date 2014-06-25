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
    mr.rmdir('wordcount/')
except OSError as ose:
    print 'tried to delete helloout/ but i couldnt', ose

mr.put('small_testdata/LICENSE.txt', 'LICENSE')
mr.put('small_testdata/README.txt', 'README')

def mapf(k, v, params):
    for tok in v.strip().split():
        yield (tok.lower(), 1)

def reducef(k, vs, params):
    yield k, sum(vs)

mr.mapreduce(\
    inputs = ['LICENSE', 'README'], \
    output_dir = 'wordcount', \
    map_func = mapf, \
    reduce_func = reducef,
    num_reducers=3)
    
print mr.read('wordcount/reduce000000')
