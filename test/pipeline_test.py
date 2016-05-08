from pydis import Pydis
import logging

logging.basicConfig(level=logging.DEBUG)

def test_pipeline():
    r = Pydis('192.168.99.100', 32768)
    pipe = r.pipeline()
    pipe.pipeline_execute_command('SET', 'a', 'a1').pipeline_execute_command('GET', 'a')
    res = pipe.execute_pipeline()
    print(res)

test_pipeline()
