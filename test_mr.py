import mr
import random
import time
import shutil

mr.REDIS_DB = str(int(mr.REDIS_DB) - 1) # switch to another db as to not clobber the production one

MINICLUSTER_SLAVES = None


def start_debug_mini(num_slaves=10):

    mr.slaves(be_sure=True)

    ps = mr.start_slaves(50000 + random.randint(1000,9000), \
        '/tmp/mr.py/mini/testdir-' + str(random.randint(0,9000)) + '-', \
        num_slaves, wait=False)

    # wait for them to come up
    while len(mr.slaves(cache_expire=0, be_sure=True)) < num_slaves:
        time.sleep(.2)

    mr.format_fs(are_you_sure=True)

    return ps

def stop_debug_mini(processes):
    for p in processes:
        p.terminate()

    mr.format_fs(are_you_sure=True)

    shutil.rmtree('/tmp/mr.py/mini')

    mr.slaves(be_sure=True)

def test_empty_slaves():
    # this has to be done before the mini cluster starts
    assert(len(mr.slaves(be_sure=True)) == 0)


def test_start_mini():
    # this test needs to be close to first
    global MINICLUSTER_SLAVES

    MINICLUSTER_SLAVES = start_debug_mini()


def test_function_serde():
    def sq(x):
        return x * x

    assert(mr._deserialize_function(mr._serialize_function(sq))(3) == 9)


def test_group_sorted():
    tl = [(1, 2), (1, 3), (2, 3), (3, 9), (3, 11), (3, 3)]

    tl2 = []
    for k, vs in mr._group_sorted(tl):
        for v in vs:
            tl2.append((k, v))

    assert(tl == tl2)

    tl3 = []
    for k, vs in mr._group_sorted([]):
        tl.append(k, vs)

    assert(len(tl3) == 0)


def test_mkdirp():
    # just make some dirs and see that they work
    path1 = '/tmp/mr.py/mkdirtest/a/b/c/'+ str(random.randint(1,100000)) +'/e/f/g/h/i/j/k'
    path2 = '/tmp/mr.py/mkdirtest/a/b/e/'+ str(random.randint(1,100000)) +'/e/a/b/c/d/'
    mr._mkdirp(path1)
    mr._mkdirp(path2)

    fp = open(path1 + '/file.txt', 'w')
    fp.write('hello world')
    fp.close()
    
    # make sure it doesn't die when trying to make the same directory
    mr._mkdirp('/tmp/mr.py/mkdirtest/eee')
    mr._mkdirp('/tmp/mr.py/mkdirtest/eee')
    mr._mkdirp('/tmp/mr.py/mkdirtest/eee')

    shutil.rmtree('/tmp/mr.py/mkdirtest/')


def test_writereaddelete():

    mr.write('mrpytest/hello', 'world')
    assert(len(mr.who_has('mrpytest/hello')) == 3)
    assert(mr.read('mrpytest/hello') == 'world')

    mr.delete('mrpytest/hello')



def test_copy():

    mr.write('mrpytest/copytest', 'bees knees')
    mr.copy('mrpytest/copytest', 'mrpytest/thecopy')
    assert(mr.read('mrpytest/thecopy') == 'bees knees')


def test_write50MBfile():

    mr.write('mrpytest/50MB', 'd' * 52428800)
    assert(len(mr.who_has('mrpytest/50MB')) == 3)
    assert(mr.read('mrpytest/50MB') == 'd' * 52428800)

    mr.delete('mrpytest/50MB')


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

    
def test_stop_debug_mini():
    # this test needs to be last
    stop_debug_mini(MINICLUSTER_SLAVES)
