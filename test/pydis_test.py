from protocol import *
import logging
from pydis import Pydis

logging.basicConfig(level=logging.DEBUG)

def set_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.set('a', 1234)
    print(res)
    
def get_test():
    pydis = Pydis('192.168.99.100', 32768)
    # pydis.set_socket_read_size(2)
    res = pydis.get('c')
    print(res)
    
def mset_test():
    pydis = Pydis('192.168.99.100', 32768)
    d = {'k1':'v1', 'k2':'v2'}
    res = pydis.mset(d)
    print(res)
    
def mget_test():
    pydis = Pydis('192.168.99.100', 32768)
    pydis.set_socket_read_size(2)
    res = pydis.mget('k1', 'k2', 'k3', 'k4')
    print(res)

def ping_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.ping()
    print(res)
    
def exists_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.exists('a', 'b', 'c')
    print(res)
    
def delete_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.delete('a', 'b', 'c')
    print(res)

def type_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.type('a')
    print(res)

def keys_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.keys('c')
    print(res)

def randomkey_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.randomkey()
    print(res)

def renamenx_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.renamenx('c', 'a')
    print(res)
    
def dbsize_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.dbsize()
    print(res)

def expire_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.expire('a', 10)
    print(res)

def expireat():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.expire('a', 1555555555555)
    print(res)

def ttl():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.ttl('a')
    print(res)

def move_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.move('a', 1)
    print(res)

def getset_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.getset('a', 12)
    print(res)
    
def setnx_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.setnx('c', 56)
    print(res)
    
def setex_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.setex('c', 10, 56)
    print(res)
    
def msetnx_test():
    pydis = Pydis('192.168.99.100', 32768)
    d = {'k1':'v3', 'k2':'v3'}
    res = pydis.msetnx(d)
    print(res)

def decrby_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.decrby('d', 8)
    print(res)

def append_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.append('a', 8)
    print(res)
    
def hset_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.hset('m', 'f1', 'v1')
    print(res)

def hget_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.hget('m', 'f1')
    print(res)
    
def hmset_test():
    pydis = Pydis('192.168.99.100', 32768)
    fv = {'hmf1':'hmv1', 'hmf2':'hmv2'}
    res = pydis.hmset('hm', fv)
    print(res)

def hmget_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.hmget('hm', 'hmf1', 'hmf2')
    print(res)
    
def hvals_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.hvals('hm')
    print(res)

def hgetall_test():
    pydis = Pydis('192.168.99.100', 32768)
    res = pydis.hgetall('hm')
    print(res)
    
hgetall_test()
