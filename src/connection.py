import socket
import logging

class Connection:
    def __init__(self, host='127.0.0.1', port=6379, db=0, password=None):
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._socket = None
        self._socket_read_size = 65536
        self._timeout = 5
        self._pre_request = None
        
    def set_socket_read_size(self, size):
        self._socket_read_size = size
        
    def connect(self):
        if self._socket is None:
            self._socket = self._connect()
            if self._password is not None:
                self.send_command('AUTH', self._password)
                self._socket.recv(65536)
            if self._db is not None:
                self.send_command('SELECT', self._db)
                self._socket.recv(65536)
    
    def goto_connect(self, host='127.0.0.1', port=6379):
        self._host = host
        self._port = port
        self._socket = self._connect()
                
    def _connect(self):
        for res in socket.getaddrinfo(self._host, self._port, 0, socket.SOCK_STREAM):
            try:
                af, socktype, proto, canonname, sa = res
                sock = socket.socket(af, socktype, proto)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                sock.settimeout(self._timeout)
                sock.connect(sa)
                return sock
            except:
                if sock is not None:
                    sock.close()
            
    def send_command(self, cmd=None, *args):
        cmd_array = []
        cmd_array.append('*' + str(len(args) + 1))
        cmd_array.append('$' + str(len(cmd)))
        cmd_array.append(cmd)
        for arg in args:
            cmd_array.append('$' + str(len(str(arg))))
            cmd_array.append(str(arg))
        cmd_context = '\r\n'.join(cmd_array)
        cmd_context += '\r\n'
        logging.debug(cmd_context.encode())
        self._pre_request = cmd_context.encode()
        self._socket.send(cmd_context.encode())
        
    def send_pre_command(self):
        self._socket.send(self._pre_request)
    
    def close(self):
        if self._socket is not None:
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
            
    def recv(self):
        if self._socket is not None:
            return self._socket.recv(self._socket_read_size)
    
