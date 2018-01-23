import asyncio
import pprint
import logging
from pundun import apollo_pb2 as apollo
from pundun import utils
import scram
import sys

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
        self.message_dict = {}
        self.loop = asyncio.get_event_loop()
        (self.reader, self.writer) = self._connect(self.loop)
        self.listener_task = asyncio.ensure_future(self._listener())

    def __del__(self):
        if self.loop.is_running():
            if not self.listener_task.cancelled():
                self.listener_task.cancel()
                self._disconnect(self.loop)
            self.loop.close()

    def cleanup(self):
        logging.info('Client cleanup..')
        self.listener_task.cancel()
        self._disconnect(self.loop)
        self.loop.close()

    @asyncio.coroutine
    def _listener(self):
        logging.debug('listener started..')
        while self.loop.is_running():
            try:
                len_bytes = yield from self.reader.readexactly(4)
                length = int.from_bytes(len_bytes, byteorder='big')
                cid_bytes = yield from self.reader.readexactly(2)
                cid = int.from_bytes(cid_bytes, byteorder='big')
                data = yield from self.reader.readexactly(length-2)
                q = self.message_dict[cid]
                q.put_nowait(data)
                logging.debug('put q: %s', pprint.pformat(q))
            except:
                logging.warning('Stop listener: {}'.format(sys.exc_info()[0]))
                break
        logging.debug('listener stopped..')

    def _connect(self, loop):
        (reader, writer) = scram.connect(self.host, self.port, loop = loop)
        res = scram.authenticate(self.username, self.password,
                                 streamreader = reader,
                                 streamwriter = writer,
                                 loop = self.loop)
        logging.debug('Scrampy Auth response: {}'.format(res))
        return (reader, writer)

    def _disconnect(self, loop):
        return scram.disconnect(streamwriter = self.writer, loop = loop)

    def create_table(self, table_name, key_def, options):
        return self.loop.run_until_complete(self._create_table(table_name,
                                                               key_def,
                                                               options))

    def _create_table(self, table_name, key_def, options):
        pdu = self._make_pdu()
        pdu.create_table.table_name = table_name
        pdu.create_table.keys.extend(key_def)
        table_options = utils.make_table_options(options)
        pdu.create_table.table_options.extend(table_options)
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def delete_table(self, table_name):
        return self.loop.run_until_complete(self._delete_table(table_name))

    def _delete_table(self, table_name):
        pdu = self._make_pdu()
        pdu.delete_table.table_name = table_name
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def open_table(self, table_name):
        return self.loop.run_until_complete(self._open_table(table_name))

    def _open_table(self, table_name):
        pdu = self._make_pdu()
        pdu.open_table.table_name = table_name
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def close_table(self, table_name):
        return self.loop.run_until_complete(self._close_table(table_name))

    def _close_table(self, table_name):
        pdu = self._make_pdu()
        pdu.close_table.table_name = table_name
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def table_info(self, table_name, attributes = []):
        return self.loop.run_until_complete(
            self._table_info(table_name, attributes))

    def _table_info(self, table_name, attributes):
        pdu = self._make_pdu()
        pdu.table_info.table_name = table_name
        pdu.table_info.attributes.extend(attributes)
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def write(self, table_name, key, columns):
        return self.loop.run_until_complete(
            self._write(table_name, key, columns))

    def _write(self, table_name, key, columns):
        pdu = self._make_pdu()
        pdu.write.table_name = table_name
        key_fields = utils.make_fields(key)
        pdu.write.key.extend(key_fields)
        columns_fields = utils.make_fields(columns)
        pdu.write.columns.extend(columns_fields)
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def delete(self, table_name, key):
        return self.loop.run_until_complete(self._delete(table_name, key))

    def _delete(self, table_name, key):
        pdu = self._make_pdu()
        pdu.delete.table_name = table_name
        key_fields = utils.make_fields(key)
        pdu.delete.key.extend(key_fields)
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def update(self, table_name, key, update_operations):
        return self.loop.run_until_complete(
            self._update(table_name, key, update_operations))

    def _update(self, table_name, key, update_operations):
        pdu = self._make_pdu()
        pdu.update.table_name = table_name
        key_fields = utils.make_fields(key)
        pdu.update.key.extend(key_fields)
        uol = utils.make_update_operation_list(update_operations)
        pdu.update.update_operation.extend(uol)
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def read(self, table_name, key):
        return self.loop.run_until_complete(self._read(table_name, key))

    def _read(self, table_name, key):
        pdu = self._make_pdu()
        pdu.read.table_name = table_name
        key_fields = utils.make_fields(key)
        pdu.read.key.extend(key_fields)
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def index_read(self, table_name, column_name, term, filter):
        return self.loop.run_until_complete(
            self._index_read(table_name, column_name, term, filter))

    def _index_read(self, table_name, column_name, term, filter):
        pdu = self._make_pdu()
        pdu.index_read.table_name = table_name
        pdu.index_read.column_name = column_name
        pdu.index_read.term = term
        posting_filter = utils.make_posting_filter(filter)
        pdu.index_read.filter.sort_by = posting_filter.sort_by
        pdu.index_read.filter.start_ts = posting_filter.start_ts
        pdu.index_read.filter.end_ts = posting_filter.end_ts
        pdu.index_read.filter.max_postings = posting_filter.max_postings
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def read_range(self, table_name, start_key, end_key, limit):
        return self.loop.run_until_complete(
                self._read_range(table_name, start_key, end_key, limit))

    def _read_range(self, table_name, start_key, end_key, limit):
        pdu = self._make_pdu()
        pdu.read_range.table_name = table_name
        start_key_fields = utils.make_fields(start_key)
        pdu.read_range.start_key.extend(start_key_fields)
        end_key_fields = utils.make_fields(end_key)
        pdu.read_range.end_key.extend(end_key_fields)
        pdu.read_range.limit = limit
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def read_range_n(self, table_name, start_key, n):
        return self.loop.run_until_complete(
                self._read_range_n(table_name, start_key, n))

    def _read_range_n(self, table_name, start_key, n):
        pdu = self._make_pdu()
        pdu.read_range_n.table_name = table_name
        start_key_fields = utils.make_fields(start_key)
        pdu.read_range_n.start_key.extend(start_key_fields)
        pdu.read_range_n.n = n
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def first(self, table_name):
        return self.loop.run_until_complete(self._first(table_name))

    def _first(self, table_name):
        pdu = self._make_pdu()
        pdu.first.table_name = table_name
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def last(self, table_name):
        return self.loop.run_until_complete(self._last(table_name))

    def _last(self, table_name):
        pdu = self._make_pdu()
        pdu.last.table_name = table_name
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def seek(self, table_name, key):
        return self.loop.run_until_complete(self._seek(table_name, key))

    def _seek(self, table_name, key):
        pdu = self._make_pdu()
        pdu.seek.table_name = table_name
        key_fields = utils.make_fields(key)
        pdu.seek.key.extend(key_fields)
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def next(self, it):
        return self.loop.run_until_complete(self._next(it))

    def _next(self, it):
        pdu = self._make_pdu()
        pdu.next.it = it
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def prev(self, it):
        return self.loop.run_until_complete(self._prev(it))

    def _prev(self, it):
        pdu = self._make_pdu()
        pdu.prev.it = it
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def add_index(self, table_name, config):
        return self.loop.run_until_complete(self._add_index(table_name, config))

    def _add_index(self, table_name, config):
        pdu = self._make_pdu()
        pdu.add_index.table_name = table_name
        pdu.add_index.config.extend(utils.make_index_config_list(config))
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def remove_index(self, table_name, columns):
        return self.loop.run_until_complete(
            self._remove_index(table_name, columns))

    def _remove_index(self, table_name, columns):
        pdu = self._make_pdu()
        pdu.remove_index.table_name = table_name
        pdu.remove_index.columns.extend(columns)
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def list_tables(self):
        return self.loop.run_until_complete(self._list_tables())

    def _list_tables(self):
        pdu = self._make_pdu()
        pdu.list_tables.SetInParent()
        rpdu = yield from self._write_pdu(pdu)
        return utils.format_rpdu(rpdu)

    def _make_pdu(self):
        pdu = apollo.ApolloPdu()
        pdu.version.major = 0
        pdu.version.minor = 1
        return pdu

    def _write_pdu(self, pdu):
        pdu.transaction_id = self._get_tid()
        logging.debug('pdu: %s', pprint.pformat(pdu))
        data = pdu.SerializeToString()
        logging.debug('encoded pdu: %s', pprint.pformat(data))
        cid = self._get_cid()
        cid_bytes = cid.to_bytes(2, byteorder='big')
        length = len(data) + 2
        len_bytes = length.to_bytes(4, byteorder='big')
        logging.debug('len_bytes: %s', pprint.pformat(len_bytes))
        logging.debug('cid_bytes: %s', pprint.pformat(cid_bytes))
        msg = b''.join([len_bytes,cid_bytes, data])
        logging.debug('send bytes %s', pprint.pformat(msg))
        self.writer.write(msg)
        q = asyncio.Queue(maxsize = 1, loop=self.loop)
        self.message_dict[cid] = q
        rdata = yield from q.get()
        logging.debug('received data: %s', pprint.pformat(rdata))
        del self.message_dict[cid]
        rpdu = apollo.ApolloPdu()
        rpdu.ParseFromString(rdata)
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
