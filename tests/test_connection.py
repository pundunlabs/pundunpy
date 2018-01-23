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
        timestamp = lambda: int(round(time.time()))
        start_ts = timestamp()
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
        self.assertEqual(client.read(table_name, key1),
                         ('system', '{error,not_found}'))
        index_config1 = {'column': 'name'}
        index_config2 = {'column': 'text',
                         'index_options': {
                            'char_filter': enum.CharFilter.nfc,
                            'tokenizer': enum.Tokenizer.unicode_word_boundaries,
                            'token_filter': {
                                'transform': enum.TokenTransform.casefold,
                                'add': [],
                                'delete': [],
                                'stats': enum.TokenStats.position
                                }
                            }
                        }
        config = [index_config1, index_config2]
        self.assertTrue(client.add_index(table_name, config))
        data1 = {'name': 'Erdem Aksu',
                 'text': 'Husband Dad and Coder'}
        self.assertTrue(client.write(table_name, key1, data1))
        num = 0x123456789ABCDEF0
        data2 = {'text': 'Some irrelevant text here and there',
                 'is_data': True,
                 'some_int': 900,
                 'bin': utils.uIntToBinaryDefault(num),
                 'blank': None,
                 'double': 99.45}
        self.assertTrue(client.write(table_name, key2, data2))
        #Succesful Read Operations
        self.assertEqual(client.read(table_name, key1), data1)
        self.assertEqual(client.read(table_name, key2), data2)
        posting_list1 = client.index_read(table_name, 'name', 'Erdem Aksu', {
            'sort_by': enum.SortBy.relevance,
            'start_ts': start_ts,
            #'end_ts': timestamp(),
            'max_postings': 5})
        self.assertEqual(posting_list1[0].get('key'), key1)
        posting_list2 = client.index_read(table_name, 'text', 'here', {
            'sort_by': enum.SortBy.relevance,
            'start_ts': start_ts,
            'end_ts': timestamp(),
            'max_postings': 5})
        self.assertEqual(posting_list2[0].get('key'), key2)
        expected_range_res =[(key2, data2), (key1, data1)]
        read_range_res = client.read_range(table_name, key2, key1, 10)
        self.assertEqual(read_range_res['key_columns_list'], expected_range_res)
        read_range_n_res = client.read_range_n(table_name, key2, 2)
        self.assertEqual(read_range_n_res['key_columns_list'],
                         expected_range_res)
        ##Iterator operations
        kcp_it2 = client.first(table_name)
        self.assertEqual(kcp_it2['kcp'], (key2, data2))
        kcp1 = client.next(kcp_it2['it'])
        self.assertEqual(kcp1, (key1, data1))
        kcp_it1 = client.seek(table_name, key1)
        self.assertEqual(kcp_it1['kcp'], kcp1)
        kcp2 = client.prev(kcp_it1['it'])
        self.assertEqual(kcp2, (key2, data2))
        error = client.prev(kcp_it1['it'])
        self.assertEqual(error, ('system', '{error,invalid}'))
        self.assertEqual(client.last(table_name)['kcp'], kcp1)
        del client

if __name__ == '__main__':
    utils.setup_logging(level=logging.DEBUG)
    unittest.main()
