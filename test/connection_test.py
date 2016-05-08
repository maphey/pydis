import connection
from protocol import *
import logging

logging.basicConfig(level=logging.DEBUG)

conn = connection.Connection('192.168.99.100', 32768, 0)
conn.connect()
conn.send_command(SET, 'a', 123)
