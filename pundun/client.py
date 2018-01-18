import asyncio
import pprint
import logging
from pundun import apollo_pb2 as apollo
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
        #self.loop.run_forever()
        self.tid = 0
        self.cid = 0
        self.message_dict = {}
        self.loop = asyncio.get_event_loop()
        (self.reader, self.writer) = self._connect(self.loop)
        asyncio.ensure_future(self._listener())

    def __del__(self):
        logging.info('Client cleanup..')
        self._disconnect()
        self.loop.close()

    #@asyncio.coroutine
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
                logging.error('Listener Exception: {}'.format(sys.exc_info()[0]))
                break

    def _connect(self, loop):
        (reader, writer) = scram.connect(self.host, self.port, loop = loop)
        res = scram.authenticate(self.username, self.password,
                                 streamreader = reader,
                                 streamwriter = writer,
                                 loop = self.loop)
        logging.debug('Scrampy Auth response: {}'.format(res))
        return (reader, writer)

    def _disconnect(self):
        return scram.disconnect(streamwriter = self.writer,
                                loop = self.loop)

    def read(self, table_name, key):
        return self.loop.run_until_complete(self._read(table_name, key))

    def _read(self, table_name, key):
        pdu = self._make_pdu()
        pdu.read.table_name = table_name
        key_fields = self._make_fields(key)
        pdu.read.key.extend(key_fields)
        rpdu = yield from self._write_pdu(pdu)
        logging.debug('rpdu: %s',pprint.pformat(rpdu))
        return self._format_rpdu(rpdu)

    def _make_fields(self, tuples):
        return [self._make_field(t) for t in tuples]

    def _make_field(self, t):
        (name, value) = t
        field = apollo.Field()
        field.name = name
        if isinstance(value, bool):
            field.boolean = value
        elif isinstance(value, int):
            field.int = value
        elif isinstance(value, (bytes, bytearray)):
            field.binary = value
        elif value == None:
            field.null = None
        elif isinstance(value, int):
            field.int = value
        elif isinstance(value, float):
            field.float = value
        elif isinstance(value, str):
            field.string = value
        return field

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

    def _format_rpdu(self, pdu):
        if pdu.HasField('response'):
            return self._format_response(pdu.response)
        elif pdu.HasField('error'):
            return self._format_error(pdu.error)

    def _format_response(self, response):
            if response.HasField('ok'):
                return 'ok'
            elif response.HasField('columns'):
                return self._format_field(response.columns)
            elif response.HasField('key_columns_pair'):
                return self._format_kcp(response.key_columns_pair)
            elif response.HasField('key_columns_list'):
                return self._format_kcp(response.key_columns_list)
            elif response.HasField('proplist'):
                return self._format_field(response.proplist)
            elif response.HasField('kcp_it'):
                return self._format_kcp(response.kcp_it)
            elif response.HasField('postings'):
                return self._format_postings(response.postings)
            elif response.HasField('string_list'):
                return pdu.response.string_list

    def _format_error(self, error):
        if error.HasField('system'):
            return ('system', error.system)
        elif error.HasField('protocol'):
            return ('protocol', error.protocol)
        elif error.HasField('transport'):
            return ('transport', error.transport)
        elif error.HasField('misc'):
            return ('misc', error.misc)
