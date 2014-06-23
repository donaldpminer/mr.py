#!/usr/bin/env python

import marshal
import types
import sys
import glob
from collections import defaultdict
import os
import os.path as path
import hashlib
import rpyc
from rpyc.utils.server import ForkingServer
import redis
import errno
from socket import gethostname
import time
from threading import Thread
import logging
logging.basicConfig()

#### CONFIG ####

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 'mrpy'

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
    

def slaves(timeout=10):
    slave_dict = _get_redis().hgetall('slaves')
    curtime = _get_timestamp()

    out_hosts = []

    for host in slave_dict:
        if float(slave_dict[host]) + timeout < curtime:
            h,p = host.split(':')
            try:
                c = rpyc.connect(h, int(p))
                c.root.register()
            except Exception as e:
                print 'something seems wrong with', host, e
                print 'im going to delete', host, 'from the registry'
                _unregister(h, p)
                continue

        out_hosts.append(host)

    return out_hosts

def mapreduce(\
        slaves=None, \
        mapinput_f=empty_input, \
        map_f=identity_mapper, \
        mapcombiner_f=None, \
        mappartitioner_f=hash_partitioner, \
        mapoutput_f=stdout_kv_output, \
        redinput_f=None, \
        red_f=None, \
        redoutput_f=None):
    pass
    #if params is None: params = {}

    #conns = [ rpyc.connect(str(ip), int(port)) for ip, port in slaves ]

    #mapid = 0
    #for 

    #conn.root.map(snakes._serialize_function(snakes.localfile_linereader), snakes._serialize_function(mapf), None, None, snakes._serialize_function(snakes.stdout_output), 2, {'inputfilepath' : i})

def fsput(file_name, local_path):
    fh = open(local_path)


#### NAMENODE OPS ####

def _NN_register_file(hostname, port, file_name):
    # tell the register host:port has a file
    _get_redis().hset('file-' + file_name, _cat_host(hostname, port), _get_timestamp())

def _NN_check_exists(file_name):
    return _get_redis().exists('file-' + file_name)

#### SLAVE SERVER ####

def _register(hostname, port):
    ts = _get_timestamp()

    _get_redis().hset("slaves", \
            _cat_host(hostname, port), \
            ts)
 
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

        self.exposed_register()

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

    def exposed_save(self, file_name, payload):

        _pathcheck(file_name)

        _mkdirp('storage')

        opath = path.join('storage', file_name)
        if path.exists(opath):
            raise OSError("The file '%s' already exists" % file_name)
            return False

        of = open(opath, 'w')
        of.write(payload)
        of.close()

        _NN_register_file(SlaveServer._hostname, SlaveServer._port, file_name)

        return True

    def exposed_fetch(self, file_name):
        _pathcheck(file_name)

        return open(path.join('storage', file_name)).read()

    def exposed_delete(self, file_name):
        _pathcheck(file_name)

        os.remove(path.join('storage', file_name))

        return True

    def exposed_pushremote(self, file_name, destination):
        _pathcheck(file_name)

        c = rpyc.connect(*destination)

        c.root.put(file_name, open(path.join('storage', file_name)).read())

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
