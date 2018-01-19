# -*- coding: utf-8 -*-

import logging
from context import pundun
from pundun import Client
import unittest
from pundun import utils
from pundun import constants as enum
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

    def test_all(self):
        host = '127.0.0.1'
        port = 8887
        user = 'admin'
        secret = 'admin'
        logging.info("testing..")
        client = Client(host, port, user, secret)
        table_name = 'pundunpy_test_table'
        key1 = {'id': '0001', 'ts': time.monotonic()}
        key2 = {'id': '0002', 'ts': time.monotonic()}
        tab_exists = table_name in client.list_tables()
        if tab_exists:
            self.assertTrue(client.delete_table(table_name))
        self.assertTrue(client.create_table(table_name,
                                            ['id', 'ts'],
                                            {'num_of_shards': 1}))
        self.assertEqual(client.read('non_existing_table', key1),
                         ('system', '{error,"no_table"}'))
        self.assertEqual(client.read('pundunpy_test_table', key1),
                         ('system', '{error,not_found}'))
        index_config1 = {'column': 'name'}
        index_config2 = {'column': 'text',
                         'index_options': {
                            'char_filter': enum.CharFilter.nfc.value,
                            'tokenizer': enum.Tokenizer.unicode_word_boundaries.value,
                            'token_filter': {
                                'transform': enum.TokenTransform.casefold.value,
                                'add': [],
                                'delete': [],
                                'stats': enum.TokenStats.position.value
                                }
                            }
                        }
        config = [index_config1, index_config2]
        client.add_index('pundunpy_test_table', config)
        del client

if __name__ == '__main__':
    utils.setup_logging(level=logging.DEBUG)
    unittest.main()
