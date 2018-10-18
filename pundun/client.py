# vim: set expandtab:
import asyncio
import pprint
import logging
from pundun import apollo_pb2 as apollo
from pundun import utils
import scram
import sys
import ssl
from concurrent.futures import CancelledError

class Client:
    """Client class including pundun procedures."""

    def __init__(self, host, port, user, password):
        logging.info('Client setup..')
        self.host = host
        self.port = port
        self.username = user
        self.password = password
        self.tid = 0
        self.cid = 0
        self.writer = None
        self.reader = None
        self.message_dict = {}
        self.loop = self._get_event_loop()
        (self.reader, self.writer) = self._connect()
        if self.reader == None or self.writer == None:
            raise ValueError('Could not connect.')

        self._auth()
        asyncio.ensure_future(self._listener(), loop=self.loop)

    def __del__(self):
        self.cleanup()
        self.loop.close()

    def cleanup(self):
        self._cancel_all_tasks()
        self._disconnect()

    def _cancel_all_tasks(self):
        for task in asyncio.all_tasks(loop=self.loop):
            task.cancel()

    def _get_event_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

    def run_loop(self):
        logging.debug('Run loop forever.')
        return self.loop.run_forever()

    def stop_loop(self):
        logging.debug('Stop loop.')
        return self.loop.call_soon_threadsafe(self.loop.stop)

    def write_data(self, msg, timeout=0):
        length = len(msg)
        num_bytes = length.to_bytes(4, byteorder='big')
        data = b''.join([num_bytes, msg])
        logging.debug('send bytes %s', pprint.pformat(data))
        res = self.writer.write(data)
        return res

    async def read_data(self, timeout = None):
        if timeout == 0:
            timeout = None
        try:
            numbytes = await asyncio.wait_for(self.reader.readexactly(4),
                                              timeout=timeout,loop=self.loop)
            lenght = int.from_bytes(numbytes, byteorder='big')
            data = await asyncio.wait_for(self.reader.readexactly(lenght),
                                          timeout=timeout, loop=self.loop)
            return data

        except CancelledError as e:
            raise e
        except asyncio.TimeoutError as e:
            raise e
        except Exception as e:
            err = sys.exc_info()
            logging.warning('read error: %s', pprint.pformat(err))
            raise e

    async def _listener(self):
        logging.info('Listener started..')
        while self.loop.is_running():
            logging.debug('listener looping')
            try:
                ciddata = await self.read_data()
                cid = int.from_bytes(ciddata[:2], byteorder='big')
                q = self.message_dict.get(cid, False)
                if q:
                    q.put_nowait(ciddata[2:])
                    logging.debug('put q: %s', pprint.pformat(q))
                else:
                    logging.debug('no waiting q for cid: %d', cid)
                continue
            except CancelledError as e:
                logging.info('Listener task cancelled..')
                raise e
                break
            except Exception as e:
                print("Exceptionasdfasd {}".format(e))
                raise e
            except:
                err = sys.exc_info()
                logging.warning('Stop listener: %s', pprint.pformat(err))
                break
        logging.info('Listener stopped..')

    def _connect(self):
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        connection = asyncio.open_connection(self.host,
                                             self.port,
                                             ssl=ctx, loop=self.loop)
        (reader, writer) = self.loop.run_until_complete(connection)
        logging.debug('connected')
        return (reader, writer)

    def _auth(self):
        logging.debug('authing')
        authing = scram.authenticate(self.username, self.password,
                                 streamreader = self,
                                 streamwriter = self,
                                 loop=self.loop)
        res = self.loop.run_until_complete(authing)
        logging.debug('Scrampy Auth response: {}'.format(res))

    def _disconnect(self):
        if self.writer:
            try:
                return self.writer.close()
            except:
                return
        return

    def create_table(self, table_name, key_def, options, do_async = False):
        if do_async:
            return self._run_coroutine(
                    self._create_table(table_name, key_def, options))
        else:
            return self.loop.run_until_complete(
                    self._create_table(table_name, key_def, options))

    async def _create_table(self, table_name, key_def, options):
        pdu = self._make_pdu()
        pdu.create_table.table_name = table_name
        pdu.create_table.keys.extend(key_def)
        table_options = utils.make_table_options(options)
        pdu.create_table.table_options.extend(table_options)
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def delete_table(self, table_name, do_async = False):
        if do_async:
            return self._run_coroutine(self._delete_table(table_name))
        else:
            return self.loop.run_until_complete(self._delete_table(table_name))

    async def _delete_table(self, table_name):
        pdu = self._make_pdu()
        pdu.delete_table.table_name = table_name
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def open_table(self, table_name, do_async = False):
        if do_async:
            return self._run_coroutine(self._open_table(table_name))
        else:
            return self.loop.run_until_complete(self._open_table(table_name))

    async def _open_table(self, table_name):
        pdu = self._make_pdu()
        pdu.open_table.table_name = table_name
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def close_table(self, table_name, do_async = False):
        if do_async:
            return self._run_coroutine(self._close_table(table_name))
        else:
            return self.loop.run_until_complete(self._close_table(table_name))

    async def _close_table(self, table_name):
        pdu = self._make_pdu()
        pdu.close_table.table_name = table_name
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def table_info(self, table_name, attributes = [], do_async = False):
        if do_async:
            return self._run_coroutine(self._table_info(table_name, attributes))
        else:
            return self.loop.run_until_complete(
                    self._table_info(table_name, attributes))

    async def _table_info(self, table_name, attributes):
        pdu = self._make_pdu()
        pdu.table_info.table_name = table_name
        pdu.table_info.attributes.extend(attributes)
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def write(self, table_name, key, columns, do_async = False):
        if do_async:
            return self._run_coroutine(self._write(table_name, key, columns))
        else:
            return self.loop.run_until_complete(
                    self._write(table_name, key, columns))

    async def _write(self, table_name, key, columns):
        pdu = self._make_pdu()
        pdu.write.table_name = table_name
        key_fields = utils.make_fields(key)
        pdu.write.key.extend(key_fields)
        columns_fields = utils.make_fields(columns)
        pdu.write.columns.extend(columns_fields)
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def delete(self, table_name, key, do_async = False):
        if do_async:
            return self._run_coroutine(self._delete(table_name, key))
        else:
            return self.loop.run_until_complete(self._delete(table_name, key))

    async def _delete(self, table_name, key):
        pdu = self._make_pdu()
        pdu.delete.table_name = table_name
        key_fields = utils.make_fields(key)
        pdu.delete.key.extend(key_fields)
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def update(self, table_name, key, update_operations, do_async = False):
        if do_async:
            return self._run_coroutine(
                    self._update(table_name, key, update_operations))
        else:
            return self.loop.run_until_complete(
                    self._update(table_name, key, update_operations))

    async def _update(self, table_name, key, update_operations):
        pdu = self._make_pdu()
        pdu.update.table_name = table_name
        key_fields = utils.make_fields(key)
        pdu.update.key.extend(key_fields)
        uol = utils.make_update_operation_list(update_operations)
        pdu.update.update_operation.extend(uol)
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def read(self, table_name, key, do_async = False):
        if do_async:
            return self._run_coroutine(self._read(table_name, key))
        else:
            return self.loop.run_until_complete(self._read(table_name, key))

    async def _read(self, table_name, key):
        pdu = self._make_pdu()
        pdu.read.table_name = table_name
        key_fields = utils.make_fields(key)
        pdu.read.key.extend(key_fields)
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def index_read(self, table_name, column_name, term, filter, do_async = False):
        if do_async:
            return self._run_coroutine(
                    self._index_read(table_name, column_name, term, filter))
        else:
            return self.loop.run_until_complete(
                    self._index_read(table_name, column_name, term, filter))

    async def _index_read(self, table_name, column_name, term, filter):
        pdu = self._make_pdu()
        pdu.index_read.table_name = table_name
        pdu.index_read.column_name = column_name
        pdu.index_read.term = term
        posting_filter = utils.make_posting_filter(filter)
        pdu.index_read.filter.sort_by = posting_filter.sort_by
        pdu.index_read.filter.start_ts = posting_filter.start_ts
        pdu.index_read.filter.end_ts = posting_filter.end_ts
        pdu.index_read.filter.max_postings = posting_filter.max_postings
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def read_range(self, table_name, start_key, end_key, limit, do_async = False):
        if do_async:
            return self._run_coroutine(
                    self._read_range(table_name, start_key, end_key, limit))
        else:
            return self.loop.run_until_complete(
                    self._read_range(table_name, start_key, end_key, limit))

    async def _read_range(self, table_name, start_key, end_key, limit):
        pdu = self._make_pdu()
        pdu.read_range.table_name = table_name
        start_key_fields = utils.make_fields(start_key)
        pdu.read_range.start_key.extend(start_key_fields)
        end_key_fields = utils.make_fields(end_key)
        pdu.read_range.end_key.extend(end_key_fields)
        pdu.read_range.limit = limit
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def read_range_n(self, table_name, start_key, n, do_async = False):
        if do_async:
            return self._run_coroutine(
                    self._read_range_n(table_name, start_key, n))
        else:
            return self.loop.run_until_complete(
                    self._read_range_n(table_name, start_key, n))

    async def _read_range_n(self, table_name, start_key, n):
        pdu = self._make_pdu()
        pdu.read_range_n.table_name = table_name
        start_key_fields = utils.make_fields(start_key)
        pdu.read_range_n.start_key.extend(start_key_fields)
        pdu.read_range_n.n = n
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def first(self, table_name, do_async = False):
        if do_async:
            return self._run_coroutine(self._first(table_name))
        else:
            return self.loop.run_until_complete(self._first(table_name))

    async def _first(self, table_name):
        pdu = self._make_pdu()
        pdu.first.table_name = table_name
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def last(self, table_name, do_async = False):
        if do_async:
            return self._run_coroutine(self._last(table_name))
        else:
            return self.loop.run_until_complete(self._last(table_name))

    async def _last(self, table_name):
        pdu = self._make_pdu()
        pdu.last.table_name = table_name
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def seek(self, table_name, key, do_async = False):
        if do_async:
            return self._run_coroutine(self._seek(table_name, key))
        else:
            return self.loop.run_until_complete(self._seek(table_name, key))

    async def _seek(self, table_name, key):
        pdu = self._make_pdu()
        pdu.seek.table_name = table_name
        key_fields = utils.make_fields(key)
        pdu.seek.key.extend(key_fields)
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def next(self, it, do_async = False):
        if do_async:
            return self._run_coroutine(self._next(it))
        else:
            return self.loop.run_until_complete(self._next(it))

    async def _next(self, it):
        pdu = self._make_pdu()
        pdu.next.it = it
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def prev(self, it, do_async = False):
        if do_async:
            return self._run_coroutine(self._prev(it))
        else:
            return self.loop.run_until_complete(self._prev(it))

    async def _prev(self, it):
        pdu = self._make_pdu()
        pdu.prev.it = it
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def add_index(self, table_name, config, do_async = False):
        if do_async:
            return self.loop._run_coroutine(
                    self._add_index(table_name, config))
        else:
            return self.loop.run_until_complete(
                    self._add_index(table_name, config))

    async def _add_index(self, table_name, config):
        pdu = self._make_pdu()
        pdu.add_index.table_name = table_name
        pdu.add_index.config.extend(utils.make_index_config_list(config))
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def remove_index(self, table_name, columns, do_async = False):
        if do_async:
            return self._run_coroutine(
                    self._remove_index(table_name, columns))
        else:
            return self.loop.run_until_complete(
                    self._remove_index(table_name, columns))

    async def _remove_index(self, table_name, columns):
        pdu = self._make_pdu()
        pdu.remove_index.table_name = table_name
        pdu.remove_index.columns.extend(columns)
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def list_tables(self, do_async = False):
        if do_async:
            return self._run_coroutine(self._list_tables())
        else:
            return self.loop.run_until_complete(self._list_tables())

    async def _list_tables(self):
        pdu = self._make_pdu()
        pdu.list_tables.SetInParent()
        rpdu = await self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def _run_coroutine(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def _make_pdu(self):
        pdu = apollo.ApolloPdu()
        pdu.version.major = 0
        pdu.version.minor = 1
        return pdu

    async def _write_pdu(self, pdu):
        pdu.transaction_id = self._get_tid()
        logging.debug('pdu: %s', pprint.pformat(pdu))
        data = pdu.SerializeToString()
        logging.debug('encoded pdu: %s', pprint.pformat(data))
        cid = self._get_cid()
        cid_bytes = cid.to_bytes(2, byteorder='big')
        logging.debug('cid_bytes: %s', pprint.pformat(cid_bytes))
        msg = b''.join([cid_bytes, data])
        self.write_data(msg)
        q = asyncio.Queue(maxsize = 1, loop=self.loop)
        self.message_dict[cid] = q
        coro = asyncio.Task(q.get(), loop=self.loop)
        rpdu = apollo.ApolloPdu()
        try:
            rdata = await asyncio.wait_for(coro, timeout=60, loop=self.loop)
            logging.debug('received data: %s', pprint.pformat(rdata))
            rpdu.ParseFromString(rdata)
        except asyncio.TimeoutError:
            rpdu.error.transport = 'timeout'
        del self.message_dict[cid]
        return rpdu

    def _get_tid(self):
        tid = self.tid
        if self.tid == 4294967295:
            self.tid = 0
        else:
            self.tid += 1
        return tid

    def _get_cid(self):
        cid = self.cid
        if self.cid == 65535:
            self.cid = 0
        else:
            self.cid += 1
        return cid
