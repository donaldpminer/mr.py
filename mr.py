#!/usr/bin/env python

import flask
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

#### CONFIG ####



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

def _mkdirp(dpath):
    try:
        os.mkdir(dpath)
    except OSError, e:
        if e.errno == 17:  # already exists
            pass
        else:
            raise e

def _pathcheck(file_name):
    for c in file_name:
        if not c.isalnum():
            raise ValueError("This file name '%s' is not valid, we only accept letters or numbers")

#### STANDARD LIBRARY ####

def Reducer(object):
    pass

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

####  API ####
    
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


#### SERVER ####

class Server(rpyc.Service):
    def exposed_ping(self):
        return 'pong'

    def on_connect(self):
        pass

    def exposed_exe(self, serialized_function, args=None):
        if args == None: args = []

        return _deserialize_function(serialized_function)(*args)

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

    def exposed_put(self, file_name, payload):
        _pathcheck(file_name)

        _mkdirp('storage')

        opath = path.join('storage', file_name)
        if path.exists(opath):
            raise OSError("The file '%s' already exists" % file_name)
            return False

        of = open(opath, 'w')
        of.write(payload)
        of.close()

        return True

    def exposed_get(self, file_name):
        _pathcheck(file_name)

        return open(path.join('storage', file_name)).read()

    def exposed_delete(self, file_name):
        _pathcheck(file_name)

        os.remove(path.join('storage', file_name))

        return True

    def exposed_copyremote(self, file_name, destination):
        _pathcheck(file_name)

        c = rpyc.connect(*destination)

        c.root.put(file_name, open(path.join('storage', file_name)).read())

        return True

#### MAIN ####

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print 'asf'
    elif sys.argv[1] == 'server':
        data_dir = path.abspath(sys.argv[3])
        _mkdirp(data_dir)
        os.chdir(data_dir)

        server = ForkingServer(Server, port=int(sys.argv[2]))

        server.start()
    else:
        print 'you did something wrong'
