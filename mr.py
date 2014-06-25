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



logging.basicConfig()

#### CONFIG ####

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 'mrpy'

USE_SLAVES_CACHE = True

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
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def _pathcheck(file_name):
    for c in file_name:
        if not c.isalnum():
            raise ValueError("This file name '%s' is not valid, we only accept letters or numbers")

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
    except Exception as e:
        print 'there was a problem connecting to', hostname, port, ':', e
        print '   ... i\'m going to unregister it'

        _unregister(hostname, port)

    return a


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

def devnull_output(reducer_number, payload):
    pass # "pass" is intentional. this is not a stub.

def localdir_output(path):
    pass


#### CLIENTS and APIs ####

_SLAVES_CACHE = None
_SLAVES_CACHE_TS = 0

def slaves(timeout=30, cache_expire=60):
    global _SLAVES_CACHE
    global _SLAVES_CACHE_TS

    if USE_SLAVES_CACHE and _get_timestamp() < _SLAVES_CACHE_TS + cache_expire:
        return _SLAVES_CACHE

    slave_dict = _get_redis().hgetall('slaves')
    curtime = _get_timestamp()

    out_hosts = []

    for host in slave_dict:
        if float(slave_dict[host]) + timeout < curtime:
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

    return out_hosts

def random_slave(*k, **kv):
    return random.choice(slaves(*k, **kv))

#### FILE SYSTEM OPS ####

def _register_file(hostname, port, file_name):
    # tell the register host:port has a file
    _get_redis().hset('file-' + file_name, _cat_host(hostname, port), _get_timestamp())

def _unregister_file(hostname, port, file_name):
    # tell the register that host:port no longer has a file
    _get_redis().hdel('file-' + file_name, _cat_host(hostname, port))

def check_exists(file_name):
    _pathcheck(file_name)
    return _get_redis().exists('file-' + file_name)

def who_has(file_name):
    _pathcheck(file_name)

    if not check_exists(file_name):
        raise OSError('The file "%s" does not exist' % file_name)

    return _get_redis().hkeys('file-' + file_name)

def write(file_name, payload):
    _pathcheck(file_name)

    if check_exists(file_name):
        raise OSError('The file "%s" already exists' % file_name)

    a = _connect(*_split_hostport(random.choice(slaves())))
    a.root.save(file_name, payload)

def put(local_file, file_name):
    _pathcheck(file_name)

    write(file_name, open(local_file).read())

def delete(file_name):
    _pathcheck(file_name)

    for hostport in who_has(file_name):
        a = _connect(*_split_hostport(hostport))
        a.root.delete(file_name)

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
    return [ f.split('-', 1)[1] for f in _get_redis().keys('file-' + file_glob) ]


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
    os.chdir(data_dir)

    # store some config for later
    #  (i don't really have anywhere better to put it)
    SlaveServer._port = int(port)
    SlaveServer._hostname = gethostname()
    SlaveServer._datadir = data_dir

    s = ForkingServer(SlaveServer, port=int(port))
    _register(SlaveServer._hostname, SlaveServer._port)
    s.start()

    _unregister(SlaveServer._hostname, SlaveServer._port)

class SlaveServer(rpyc.Service):

            ## general utility commands ##

    def exposed_ping(self):
        return 'pong'

    def on_connect(self):
        print 'someone just connected'

        self.exposed_register()

    def on_disconnect(self):
        print 'someone just disconnceted'

    def exposed_register(self):
        # register with redis registry
        _register(self._hostname, self._port)

            ## mapreduce commands ##

    def exposed_map(self, \
                    iam, input_func, map_func, \
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
            output_func(reducer_num, buckets[reducer_num], params)

        return True

               ## file system commands ##

    def exposed_save(self, file_name, payload, replicate=(REPLICATION_FACTOR - 1)):

        _pathcheck(file_name)

        _mkdirp('storage')

        opath = path.join('storage', file_name)
        if path.exists(opath):
            raise OSError("The file '%s' already exists" % file_name)
            return False

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

        return open(path.join('storage', file_name)).read()

    def exposed_delete(self, file_name):
        _pathcheck(file_name)

        _unregister_file(SlaveServer._hostname, SlaveServer._port, file_name)
        os.remove(path.join('storage', file_name))

        return True

    def exposed_pushremote(self, file_name, destination):
        _pathcheck(file_name)

        c = _connect(*destination)
        c.root.save(file_name, open(path.join('storage', file_name)).read())

        return True

                              ## slave code ends here ##

#### MAIN ####

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print 'asf'
    elif sys.argv[1] == 'slave':
        start_slave(sys.argv[2], sys.argv[3])

    else:
        print 'you did something wrong'
