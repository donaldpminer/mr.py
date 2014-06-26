#!/usr/bin/env python

import marshal
import types
import sys
import glob
import os
import os.path as path
import hashlib
import rpyc
from rpyc.utils.server import ForkingServer
import redis
import errno
from socket import gethostname
import time
import logging
import random
from multiprocessing import Process
from heapq import merge


logging.basicConfig()

#### CONFIG ####

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = '10'

USE_SLAVES_CACHE = False # this doesn't seem to work too well, don't use it

REPLICATION_FACTOR = 3

#### UTIL FUNCTIONS ####

def _serialize_function(func):
    return marshal.dumps(func.func_code)

def _deserialize_function(sfunc):
    return types.FunctionType(marshal.loads(sfunc), globals())

def _group_sorted(list_of_kv):
    if len(list_of_kv) == 0:
        return

    cur_k = list_of_kv[0][0]
    cur_vs = []
    for k, v in list_of_kv:
        if k != cur_k:
            yield cur_k, cur_vs
            cur_k = k
            cur_vs = []

        cur_vs.append(v)

    yield cur_k, cur_vs

def _mkdirp(path):
    path = path.strip()
    if len(path) == 0:
        raise ValueError("I can't make a directory with no name")

    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise exc

def _pathcheck(file_name):
    file_name = file_name.strip()
    if len(file_name) == 0:
        raise ValueError("The file name cannot be empty")

    for token in file_name.split('/'):
        if not token.isalnum():
            raise ValueError("This file name '%s' is not valid, we only accept letters or numbers that are unempty" % file_name)

def _get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

def _get_timestamp():
    return time.time()

def _cat_host(hostname, port):
    return str(hostname) + ':' + str(port)

def _split_hostport(hostnameport):
    h, p = hostnameport.split(':')
    return h, int(p)

def _connect(hostname, port=None):
    if port is None:
        hostname, port = hostname.split(':')

    try:
        a = rpyc.connect(str(hostname), int(port))
        return a
    except Exception as e:
        print 'there was a problem connecting to', hostname, port, ':', e
        print '   ... i\'m going to unregister it'

        _unregister(hostname, port)

        raise e

    


#### STANDARD LIBRARY ####

def empty_input(params):
    return
    yield

def identity_mapper(key, val, params):
    yield key, val

def identity_reducer(key, values, params):
    for v in values:
       yield key, v 

def hash_partitioner(value, num_reducers, params):
    return int(hashlib.sha1(str(value.__hash__())).hexdigest(), 16) % num_reducers

def localfile_linereader(params):
    return enumerate(open(params['inputfilepath']).xreadlines())

def dfs_linereader(params):
    return enumerate(read(params['inputfilepath']).splitlines())

def stdout_kv_output(reducer_number, payload, params):
    print "=" * 40
    print "Reducer number %d got this payload:" % reducer_number
    print marshal.dumps(payload)
    print "=" * 40
    print

def basic_mapoutput(reducer_number, payload, params):
    write('%s/tmp/%s/%s' % (params['outputdir'], reducer_number, params['inputfilepath']), marshal.dumps(payload))

def dfs_line_output(reducer_number, payload, params):
    out_str = '\n'.join( str(k) + '\t' + str(v) for k, v in payload )

    write('%s/reduce%s' % (params['outputdir'], str(reducer_number).zfill(6)), out_str)

def devnull_output(reducer_number, payload, params):
    pass # "pass" is intentional. this is not a stub.

def basic_shuffle(reducer_number, params):
    inputs = ls('%s/tmp/%d/*' % (params['outputdir'], int(reducer_number)))

    return [ marshal.loads(read(inp)) for inp in inputs ]

def basic_sort(mapper_outputs, params):
    # mapper_outputs is a list of k,v sets that are sorted
    cur_vs = []
    cur_k = None

    a = []
    for p in merge(mapper_outputs):
        a.append(p)

    for k, v in merge(*mapper_outputs):
        if cur_k is None:
            cur_k = k

        if cur_k != k:
            yield cur_k, cur_vs
            cur_k = k
            cur_vs = []

        cur_vs.append(v)

    yield cur_k, cur_vs

#### CLIENTS and APIs ####

_SLAVES_CACHE = None
_SLAVES_CACHE_TS = 0

def slaves(timeout=30, cache_expire=60, be_sure=False):
    global _SLAVES_CACHE
    global _SLAVES_CACHE_TS

    if USE_SLAVES_CACHE and _get_timestamp() < _SLAVES_CACHE_TS + cache_expire:
        return _SLAVES_CACHE

    slave_dict = _get_redis().hgetall('slaves')
    curtime = _get_timestamp()

    out_hosts = []

    for host in slave_dict:
        if be_sure or float(slave_dict[host]) + timeout < curtime:
            h,p = host.split(':')
            try:
                c = _connect(h, int(p))
                c.root.register()
            except Exception as e:
                print 'something seems wrong with', host, e
                print 'im going to delete', host, 'from the registry'
                _unregister(h, p)
                continue

        out_hosts.append(host)

    _SLAVES_CACHE = out_hosts
    _SLAVES_CACHE_TS = _get_timestamp()

    return sorted(out_hosts)

def random_slave(*k, **kv):
    ss = slaves(*k, **kv)
    if len(ss) == 0:
        raise OSError('There are no slaves currently running, so I can\'t randomly select one')
    
    return random.choice(ss)

#### FILE SYSTEM OPS ####

def _register_file(hostname, port, file_name):
    # tell the register host:port has a file
    r = _get_redis()

    r.hset('file-' + file_name, _cat_host(hostname, port), _get_timestamp())
    # we need to have a dummy value because otherwise redis throws away the file
    #  once it has zero replicas
    r.hset('file-' + file_name, '!', '!') 

def _unregister_file(hostname, port, file_name):
    # tell the register that host:port no longer has a file
    _get_redis().hdel('file-' + file_name, _cat_host(hostname, port))

def format_fs(are_you_sure=False):
    if not are_you_sure:
        raise ValueError("you have to call format_fs(are_you_sure=True) in order to format")

    for f in ls():
        delete(f)

def check_exists(file_name):
    _pathcheck(file_name)
    return _get_redis().exists('file-' + file_name)

def who_has(file_name):
    _pathcheck(file_name)

    if not check_exists(file_name):
        raise OSError('The file "%s" does not exist' % file_name)

    return [ h for h in _get_redis().hkeys('file-' + file_name) if h != '!' ]

def write(file_name, payload):
    _pathcheck(file_name)

    if check_exists(file_name):
        raise OSError('The file "%s" already exists' % file_name)

    a = _connect(*_split_hostport(random_slave()))
    a.root.save(file_name, payload)

def put(local_file, file_name):
    _pathcheck(file_name)

    write(file_name, open(local_file).read())

def delete(file_name):
    _pathcheck(file_name)

    _get_redis().hdel('file-' + file_name, '!')

    for hostport in who_has(file_name):
        try:
            a = _connect(*_split_hostport(hostport))
            a.root.delete(file_name)
        except Exception as e:
            print "I tried to delete", file_name, "from", hostport, \
                "but he seems to be gone... I'm going to unregister this file from this host."

            _unregister_file( *(_split_hostport(hostport) + (file_name,)) )

def deletes(file_glob):
    files_to_del = _get_redis().keys('file-' + file_glob)
    for f in files_to_del:
        delete(f.split('-', 1)[1])

def rmdir(directory_name):
    files_to_del = _get_redis().keys('file-' + directory_name.rstrip('/') + '/*')
    if len(files_to_del) == 0: raise OSError(directory_name + ' does not exist')
    for f in files_to_del:
        delete(f.split('-', 1)[1])

def read(file_name):
    _pathcheck(file_name)

    if not check_exists(file_name):
        raise OSError('The file "%s" does not exist' % file_name)

    a = _connect(*_split_hostport(random.choice(who_has(file_name))))
    return a.root.fetch(file_name)

def get(file_name, local_file):
    _pathcheck(file_name)

    open(local_file, 'w').write(read(file_name))

def ls(file_glob = '*'):
    output = []
    for f in _get_redis().keys('file-' + file_glob):
        output.append(f.split('-', 1)[1])

    return sorted(output)

def ll(file_glob = '*'):
    output = []
    for f in _get_redis().keys('file-' + file_glob):
        fn = f.split('-', 1)[1]
        output.append((fn, who_has(fn)) )

    return sorted(output)

#### MAPREDUCE ####

def mapreduce(inputs, output_dir, \
    input_func=dfs_linereader, map_func=identity_mapper, \
    combiner_func=identity_reducer, partitioner_func=hash_partitioner, \
    mapout_func=basic_mapoutput, \
    shuffle_func=basic_shuffle, sort_func=basic_sort, \
    reduce_func=identity_reducer, output_func=dfs_line_output, \
    num_reducers=1, params=None):

    sf = _serialize_function # in retrospect, this function name was too long

    sif = sf(input_func)
    smf = sf(map_func)
    scf = sf(combiner_func)
    spf = sf(partitioner_func)
    smof = sf(mapout_func)
    sshf = sf(shuffle_func)
    ssof = sf(sort_func)
    srf = sf(reduce_func)
    sof = sf(output_func)
    if params is None: params = {}

    params['outputdir'] = output_dir


    print 'map stage starting'

    map_tasks = []
    for ins in inputs:
        for f in ls(ins):
            params['inputfilepath'] = f
            p = Process(target=_start_map, args=(sif, smf, scf, spf, smof, num_reducers, params))
            p.start()
            map_tasks.append(p)

    for p in map_tasks:
        p.join()

    print 'map stage complete'

    print 'reduce stage starting'

    reduce_tasks = []
    for ri in range(num_reducers):
        p = Process(target=_start_reduce, args=(ri, sshf, ssof, srf, sof, params))
        p.start()
        reduce_tasks.append(p)

    for p in reduce_tasks:
        p.join()

    print 'reduce stage complete'
    


def _start_map(input_func, map_func, combiner_func, partitioner_func, mapout_func, num_reducers, params):

    conn = _connect(random.choice(who_has(params['inputfilepath'])))

    conn.root.map(input_func, map_func, combiner_func, partitioner_func, mapout_func, num_reducers, params)

def _start_reduce(reducer_num, shuffle_func, sort_func, reduce_func, output_func, params):

    conn = _connect(random_slave())

    conn.root.reduce(reducer_num, shuffle_func, sort_func, reduce_func, output_func, params)


#### SLAVE SERVER ####

def _register(hostname, port):
    ts = _get_timestamp()

    _get_redis().hset("slaves", _cat_host(hostname, port), ts)
 
    return ts

def _unregister(hostname, port):
    _get_redis().hdel('slaves', _cat_host(hostname, port))

def start_slave(port, data_dir):

    # set up data directory
    data_dir = path.abspath(data_dir)
    _mkdirp(data_dir)

    # store some config for later
    #  (i don't really have anywhere better to put it)
    SlaveServer._port = int(port)
    SlaveServer._hostname = gethostname()
    SlaveServer._datadir = data_dir

    _mkdirp(path.join(data_dir, 'storage'))

    s = ForkingServer(SlaveServer, port=int(port))
    _register(SlaveServer._hostname, SlaveServer._port)
    s.start()

    _unregister(SlaveServer._hostname, SlaveServer._port)

def start_slaves(start_port, data_dir_root, num_slaves, wait=True):
    ports = range(int(start_port), int(start_port) + int(num_slaves))
    processes = []
    for port in ports:
        p = Process(target=start_slave, args=(port, data_dir_root + str(port)))
        p.start()
        processes.append(p)

    if wait:
        for p in processes:
            p.join()

    else:
        return processes

class SlaveServer(rpyc.Service):

            ## general utility commands ##

    def exposed_ping(self):
        return 'pong'

    def on_connect(self):
        print self._hostname, self._port, self._datadir, 'someone just connected'

        self.exposed_register()

    def on_disconnect(self):
        print 'someone just disconnceted'

    def exposed_register(self):
        # register with redis registry
        _register(self._hostname, self._port)

            ## mapreduce commands ##

    def exposed_map(self, \
                    input_func, map_func, \
                    combiner_func, partitioner_func, \
                    output_func, num_reducers, params):

        input_func = _deserialize_function(input_func)
        map_func = _deserialize_function(map_func)
        combiner_func = _deserialize_function(combiner_func)
        partitioner_func = _deserialize_function(partitioner_func)
        output_func = _deserialize_function(output_func)

        map_out = []

        for k,v in input_func(params):
            for k,v in map_func(k, v, params):
                map_out.append((k, v))

        map_out.sort()
        
        if combiner_func != None:
            combiner_out = []
            for k, vs in _group_sorted(map_out):
                for k, v in combiner_func(k, vs, params):
                    combiner_out.append((k,v))
            map_out = combiner_out

        buckets = {} 
        for k, v in map_out:
            reducer_num = partitioner_func(k, num_reducers, params)
            buckets.setdefault(reducer_num, [])
            buckets[reducer_num].append((k,v))

        for reducer_num in buckets:
            output_func(reducer_num, sorted(buckets[reducer_num]), params)

        return True


    def exposed_reduce(self, \
                    reducer_num, shuffle_func, sort_func, reduce_func, \
                    output_func, params):

               ## file system commands ##
        shuffle_func = _deserialize_function(shuffle_func)
        sort_func = _deserialize_function(sort_func)
        reduce_func = _deserialize_function(reduce_func)
        output_func = _deserialize_function(output_func)

        reduce_out = []
        for k, vs in sort_func(shuffle_func(reducer_num, params), params):
            for k, v in reduce_func(k, vs, params):
                reduce_out.append((k, v))

        output_func(reducer_num, reduce_out, params)
        

    def exposed_save(self, file_name, payload, replicate=(REPLICATION_FACTOR - 1)):

        _pathcheck(file_name)

        opath = path.join(self._datadir, 'storage', file_name)
        if path.exists(opath):
            raise OSError("The file '%s' already exists" % file_name)
            return False

        _mkdirp(path.split(opath)[0])
        of = open(opath, 'w')
        of.write(payload)
        of.close()

        _register_file(SlaveServer._hostname, SlaveServer._port, file_name)

        if replicate > 0:
            # select a replication target of someone other than someone who has it
            target = random.sample(set(slaves()) - set(who_has(file_name)), 1)[0]

            c = _connect(*_split_hostport(target))
            c.root.save(file_name, payload, replicate - 1)

        return True

    def exposed_fetch(self, file_name):
        _pathcheck(file_name)

        try:
            return open(path.join(self._datadir, 'storage', file_name)).read()
        except IOError as e:
            print 'unregistering', file_name, 'because of:', e
            _unregister_file(self._hostname, self._port, file_name)
            return None

    def exposed_delete(self, file_name):
        _pathcheck(file_name)

        _unregister_file(SlaveServer._hostname, SlaveServer._port, file_name)

        try:
            os.remove(path.join(self._datadir, 'storage', file_name))
        except OSError as e:
            print "you tried to delete", file_name, "but it was already gone.", e
            return False

        return True

    def exposed_pushremote(self, file_name, destination):
        _pathcheck(file_name)

        c = _connect(*destination)
        c.root.save(file_name, open(path.join(self._datadir, 'storage', file_name)).read())

        return True

                              ## slave code ends here ##

#### MAIN ####

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print 'asf'
    elif sys.argv[1] == 'slave':
        start_slave(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'slaves':
        start_slaves(sys.argv[2], sys.argv[3], sys.argv[4])

    else:
        print 'you did something wrong'
