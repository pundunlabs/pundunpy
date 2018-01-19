import logging
import pprint
from pundun import apollo_pb2 as apollo

def setup_logging(level = logging.WARNING):
    date_fmt = '%Y-%m-%d %H:%M:%S'
    log_fmt=('%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] '
             '%(levelname)s %(message)s'
            )
    logging.basicConfig(format=log_fmt, level=level, datefmt=date_fmt)

def make_table_options(dictionary):
    return [_make_table_option(k, v) for k, v in dictionary.items()]

def _make_table_option(name, value):
    to = apollo.TableOption()
    if name == 'type':
        to.type = value
    elif name == 'data_model':
        to.data_model = value
    elif name == 'comparator':
        to.comparator = value
    elif name == 'time_series':
        to.time_series = value
    elif name == 'num_of_shards':
	    to.num_of_shards = value
    elif name == 'distributed':
        to.distributed = value
    elif name == 'replication_factor':
        to.replication_factor = value
    elif name == 'hash_exclude':
        to.hash_exlude = value
    elif name == 'hashing_method':
        to.hashing_method = value
    elif name == 'ttl':
        to.ttl = value
    return to

def make_fields(dictionary):
    return [make_field(k, v) for k, v in dictionary.items()]

def make_field(name, value):
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
        field.double = value
    elif isinstance(value, str):
        field.string = value
    return field

def make_index_config_list(config):
    return [make_index_config(c) for c in config]

def make_index_config(ixcfg):
    index_config = apollo.IndexConfig()
    index_config.column = ixcfg['column']
    if 'index_options' in ixcfg:
        opts = ixcfg['index_options']
        index_config.options.char_filter = opts['char_filter']
        index_config.options.tokenizer = opts['tokenizer']
        tf = opts['token_filter']
        index_config.options.token_filter.transform = tf['transform']
        index_config.options.token_filter.add.extend(tf['add'])
        index_config.options.token_filter.delete.extend(tf['delete'])
        index_config.options.token_filter.stats = tf['stats']
    return index_config

def format_rpdu(pdu):
    logging.debug('response pdu: %s',pprint.pformat(pdu))
    if pdu.HasField('response'):
        return format_response(pdu.response)
    elif pdu.HasField('error'):
        return format_error(pdu.error)

def format_response(response):
        if response.HasField('ok'):
            return True
        elif response.HasField('columns'):
            return format_field(response.columns)
        elif response.HasField('key_columns_pair'):
            return format_kcp(response.key_columns_pair)
        elif response.HasField('key_columns_list'):
            return format_kcp(response.key_columns_list)
        elif response.HasField('proplist'):
            return format_field(response.proplist)
        elif response.HasField('kcp_it'):
            return format_kcp(response.kcp_it)
        elif response.HasField('postings'):
            return format_postings(response.postings)
        elif response.HasField('string_list'):
            return response.string_list.field_names

def format_error(error):
    if error.HasField('system'):
        return ('system', error.system)
    elif error.HasField('protocol'):
        return ('protocol', error.protocol)
    elif error.HasField('transport'):
        return ('transport', error.transport)
    elif error.HasField('misc'):
        return ('misc', error.misc)
