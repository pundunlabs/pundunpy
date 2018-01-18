# -*- coding: utf-8 -*-

import logging
from context import pundun
from pundun import Client
import unittest
from pundun import utils
import time

# Try to achive parallel execution
#from concurrent.futures import ProcessPoolExecutor
#executor = ProcessPoolExecutor(2)
#    loop = asyncio.get_event_loop()
#    boo = asyncio.ensure_future(loop.run_in_executor(executor, say_boo))
#    baa = asyncio.ensure_future(loop.run_in_executor(executor, say_baa))
#
#    loop.run_forever()

class TestPundunConnection(unittest.TestCase):
    """Testing pundun connection."""

    def test_connection(self):
        host = '127.0.0.1'
        port = 8887
        user = 'admin'
        secret = 'admin'
        logging.info("testing..")
        client = Client(host, port, user, secret)
        res = client.read('test', [('key1', '1')])
        logging.info('read result: {}'.format(res))
        del client

if __name__ == '__main__':
    utils.setup_logging(level=logging.DEBUG)
    unittest.main()
