from builtins import isinstance
from connection import Connection
from protocol import *
import logging


def parse_command(cmd, *args):
    cmd_array = []
    cmd_array.append('*' + str(len(args) + 1))
    cmd_array.append('$' + str(len(cmd)))
    cmd_array.append(cmd)
    for arg in args:
        cmd_array.append('$' + str(len(str(arg))))
        cmd_array.append(str(arg))
    cmd_context = '\r\n'.join(cmd_array)
    cmd_context += '\r\n'
    return cmd_context

class Pydis(Connection):
    def __init__(self, host='127.0.0.1', port=6379, db=0, password=None):
        self._conn = Connection(host, port, db, password)
        self._conn.connect()
        self._pre_cmd = None
        self._pre_args = None
        
    def close(self):
        if self._conn:
            self._conn.close()
    
    def parse_recv(self, recv):
        if isinstance(recv, bytes):
            logging.debug(recv)
            recv_flag, recv_body = chr(recv[0]), recv[1:]
        if recv_flag == '-':
            if recv_body[:5] == b'MOVED':
                skipIpPort = recv_body.decode().strip().split(' ')[-1].split(':')
                self._conn.goto_connect(skipIpPort[0], skipIpPort[1])
                self._conn._socket.send(self._conn._pre_request);
                recv = self._conn.recv()
                return self.parse_recv(recv)
            else:
                logging.error(recv_body)
            return recv_body.decode().strip()
        elif recv_flag == '+':
            recv_list = recv_body.decode().strip().split(CRLF)
            if len(recv_list) == 1:
                return recv_list[0]
            return recv_list[1:]
        elif recv_flag == ':':
            return int(recv_body)
        elif recv_flag == '$':
            recv_list = recv_body.decode().strip().split(CRLF)
            length_flag = recv_list[0]
            if length_flag == -1:
                return None
            length = len(length_flag) + 5
            length += int(length_flag)
            while length - self._conn._socket_read_size > 0:
                recv += self._conn.recv()
                length -= self._conn._socket_read_size
            return recv.decode().strip().split(CRLF)[1:]
        elif recv_flag == '*':
            while not recv.endswith(CRLF.encode()):
                recv += self._conn.recv()
            recv_body = recv[1:].decode().strip()
            recv_list = recv_body.split(CRLF)
            while len(recv_list) < 3:
                new_recv = self._conn.recv()
                while not new_recv.endswith(CRLF.encode()):
                    new_recv += self._conn.recv()
                recv_list.extend(new_recv.decode().strip().split(CRLF))
            value_count = recv_list[0]
            res = []
            i = 1
            for x in range(int(value_count)):
                if len(recv_list) < i + 1:
                        new_recv = self._conn.recv()
                        while not new_recv.endswith(CRLF.encode()):
                            new_recv += self._conn.recv()
                        recv_list.extend(new_recv.decode().strip().split(CRLF))
                if recv_list[i] != '$-1':
                    if len(recv_list) < i + 2:
                        new_recv = self._conn.recv()
                        while not new_recv.endswith(CRLF.encode()):
                            new_recv += self._conn.recv()
                        recv_list.extend(new_recv.decode().strip().split(CRLF))
                    res.append(recv_list[i + 1])
                    i += 2
                else:
                    res.append(None)
                    i += 1
            return res
        else:
            logging.error('')
            return recv.decode()
        
    def set_socket_read_size(self, size):
        self._conn.set_socket_read_size(size)
    
    def pipeline(self):
        return Pipeline(self._conn)
    
    def select_db(self, db):
        self._conn.send_command(SELECT, db)
        recv = self._conn.recv()
        return self.parse_recv(recv)
    
    def ping(self):
        self._conn.send_command(PING)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
        
    def set(self, key, value):
        self._conn.send_command(SET, key, value)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def get(self, key):
        self._conn.send_command(GET, key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        if len(res) == 1:
            return res[0]
        else:
            return None
    
    def mset(self, keyword):
        if not isinstance(keyword, dict):
            raise Exception('parameter is ERROR')
        value = []
        for k, v in keyword.items():
            value.append(k)
            value.append(v)
        self._conn.send_command(MSET, *value)
        recv = self._conn.recv()
        return self.parse_recv(recv)

    def mget(self, *key):
        self._conn.send_command(MGET, *key)
        recv = self._conn.recv()
        values = self.parse_recv(recv)
        res = {}
        for i in range(len(key)):
            res[key[i]] = values[i]
        return res

    def exists(self, *key):
        self._conn.send_command(EXISTS, *key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def delete(self, *key):
        self._conn.send_command(DEL, *key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def type(self, key):
        self._conn.send_command(TYPE, key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def keys(self, key):
        self._conn.send_command(KEYS, key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def randomkey(self):
        self._conn.send_command(RANDOMKEY)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def rename(self, old_key, new_key):
        self._conn.send_command(RENAME, old_key, new_key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def renamenx(self, old_key, new_key):
        self._conn.send_command(RENAMENX, old_key, new_key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def dbsize(self):
        self._conn.send_command(DBSIZE)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def expire(self, key, seconds):
        self._conn.send_command(EXPIRE, key, seconds)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def expireat(self, key, timestamp):
        self._conn.send_command(EXPIREAT, key, timestamp)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def ttl(self, key):
        self._conn.send_command(TTL, key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def move(self, key, db):
        self._conn.send_command(MOVE, key, db)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def flushdb(self):
        self._conn.send_command(FLUSHDB)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def flushall(self):
        self._conn.send_command(FLUSHALL)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def getset(self, key, value):
        self._conn.send_command(GETSET, key, value)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def setnx(self, key, value):
        self._conn.send_command(SETNX, key, value)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def setex(self, key, seconds, value):
        self._conn.send_command(SETEX, key, seconds, value)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def msetnx(self, keyword):
        if not isinstance(keyword, dict):
            raise Exception('parameter is ERROR')
        value = []
        for k, v in keyword.items():
            value.append(k)
            value.append(v)
        self._conn.send_command(MSETNX, *value)
        recv = self._conn.recv()
        return self.parse_recv(recv)

    def decrby(self, key, decrement):
        self._conn.send_command(DECRBY, key, decrement)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def decr(self, key):
        self._conn.send_command(DECR, key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res

    def incrby(self, key, increment):
        self._conn.send_command(INCRBY, key, increment)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
        
    def incr(self, key):
        self._conn.send_command(INCR, key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def append(self, key, value):
        self._conn.send_command(APPEND, key, value)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
        
    def hset(self, key, field, value):
        self._conn.send_command(HSET, key, field, value)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def hget(self, key, field):
        self._conn.send_command(HGET, key, field)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def hsetnx(self, key, field, value):
        self._conn.send_command(HSETNX, key, field, value)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def hmset(self, key, field_value):
        if isinstance(field_value, dict):
            value = []
            for k, v in field_value.items():
                value.append(k)
                value.append(v)
            self._conn.send_command(HMSET, key, *value)
            recv = self._conn.recv()
            res = self.parse_recv(recv)
            return res
        else:
            return None
        
    def hmget(self, key, *fields):
        self._conn.send_command(HMGET, key, *fields)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        parse_res = {}
        for i in range(len(fields)):
            parse_res[fields[i]] = res[i]
        return parse_res
    
    def hincrby(self, key, field, increment):
        self._conn.send_command(HINCRBY, key, field, increment)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def hexists(self, key, field):
        self._conn.send_command(HEXISTS, key, field)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def hdel(self, key, *fields):
        self._conn.send_command(HDEL, key, *fields)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def hlen(self, key):
        self._conn.send_command(HLEN, key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def hkeys(self, key):
        self._conn.send_command(HKEYS, key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def hvals(self, key):
        self._conn.send_command(HVALS, key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def hgetall(self, key):
        self._conn.send_command(HGETALL, key)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        parse_res = {}
        for i in range(len(res) // 2):
            parse_res[res[i * 2]] = res[i * 2 + 1]
        return parse_res
    
    def publish(self, channel, msg):
        self._conn.send_command(PUBLISH, channel, msg)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def subscribe(self, *channels):
        self._conn.send_command(SUBSCRIBE, *channels)
        
    def unsubscribe(self, *channels):
        self._conn.send_command(UNSUBSCRIBE, *channels)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
    
    def psubscribe(self, *pattern):
        self._conn.send_command(PSUBSCRIBE, *pattern)
        
    def punsubscribe(self, *pattern):
        self._conn.send_command(PUNSUBSCRIBE, *pattern)
        recv = self._conn.recv()
        res = self.parse_recv(recv)
        return res
        
    def pubsub(self):
        return PubSub(self)
    

class Pipeline(Pydis):
    def __init__(self, connection, is_transaction=True):
        self._conn = connection
        self.command_stack = b''
        self._is_tran = is_transaction
        self._command_count = 0
        
    def pipeline_execute_command(self, cmd, *args):
        self.command_stack += parse_command(cmd, *args).encode()
        self._command_count += 1
        logging.debug(self.command_stack)
        return self
    
    def execute_pipeline(self):
        self._conn._pre_request = self.command_stack
        self._conn._socket.send(self.command_stack)
        recv = self._conn._socket.recv(self._conn._socket_read_size)
        return self.parse_recv(recv)

    def execute_transaction(self):
        multi_cmd = parse_command(MULTI).encode()
        self.command_stack = multi_cmd + self.command_stack
        exec_cmd = parse_command(EXEC).encode()
        self.command_stack += exec_cmd
        res = self.execute_pipeline()
        index = self._command_count
        self._command_count = 0
        return res[index:]


class PubSub:
    def __init__(self, pydis):
        self._client = pydis
        self._listen = True
    
    def subscribe(self, *channels):
        return self._client.subscribe(*channels)
    
    def unsubscribe(self, *channels):
        return self._client.unsubscribe(*channels)
    
    def psubscribe(self, *pattern):
        return self._client.subscribe(*pattern)
    
    def punsubscribe(self, *pattern):
        return self._client.unsubscribe(*pattern)
    
    def stop_listen(self):
        self._listen = False
    
    def listen(self):
        self._listen = True
        while self._listen:
            try:
                res = self._client._conn.recv()
                if res:
                    return self._client.parse_recv(res)
            except:
                pass

    def close(self):
        if self._client:
            self._client.close()
