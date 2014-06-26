import mr
import random
import time
import shutil

mr.REDIS_DB = str(int(mr.REDIS_DB) - 1) # switch to another db as to not clobber the production one



def start_debug_mini(num_slaves=10):

    ps = mr.start_slaves(50000 + random.randint(1000,9000), \
        '/tmp/mr.py/mini/testdir-' + str(random.randint(0,9000)) + '-', \
        num_slaves, wait=False)

    # wait for them to come up
    while len(mr.slaves(cache_expire=0, be_sure=True)) < num_slaves:
        time.sleep(.2)



    return ps

def stop_debug_mini(processes):
    for p in processes:
        p.terminate()

    shutil.rmtree('/tmp/mr.py/mini')


def test_stopstart():
    ps = start_debug_mini()

    assert(len(mr.slaves(cache_expire=0, be_sure=True)) == len(ps))

    stop_debug_mini(ps)
   
    assert(len(mr.slaves(cache_expire=0, be_sure=True)) == 0)


def test_wordcount():

    ps=start_debug_mini()

    print ps, mr.slaves(be_sure = True)

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

    stop_debug_mini(ps)
