import mr
import random

def test_wordcount():
    r = str(random.randint(0,1000000))

    mr.write('wc' + r + '/f1', ('cat dog dog ' * 10 + '\n') * 10)
    mr.write('wc' + r + '/f2', ('foo bar baz\n' * 10000))

    def mapf(k, v, params):
        for tok in v.strip().split():
            yield (tok.lower(), 1)

    def reducef(k, vs, params):
        yield k, sum(vs)

    mr.mapreduce(\
        inputs = ['wc' + r + '/f1', 'wc' + r + '/f2'], \
        output_dir = 'wordcount' + r, \
        map_func = mapf, \
        reduce_func = reducef,
        num_reducers=3)	
        
    mr.rmdir('wordcount' + r)
