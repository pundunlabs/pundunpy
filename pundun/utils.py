import logging
import pprint
from pundun import apollo_pb2 as apollo
from pundun import constants as enum

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
        field.value.boolean = value
    elif isinstance(value, int):
        field.value.int = value
    elif isinstance(value, (bytes, bytearray)):
        field.value.binary = value
    elif value == None:
        field.value.null = b''
    elif isinstance(value, float):
        field.value.double = value
    elif isinstance(value, str):
        field.value.string = value
    elif isinstance(value, list):
        values = [make_value(v) for v in value]
        field.value.list.values.extend(values)
    elif isinstance(value, dict):
        for k, v in value.items():
            field.value.map.values[k].CopyFrom(make_value(v))
    return field

def make_value(val):
    value = apollo.Value()
    if isinstance(val, bool):
        value.boolean = val
    elif isinstance(val, int):
        value.int = val
    elif isinstance(val, (bytes, bytearray)):
        value.binary = val
    elif val == None:
        value.null = b''
    elif isinstance(val, float):
        value.double = val
    elif isinstance(val, str):
        value.string = val
    elif isinstance(val, list):
        values = [make_value(v) for v in val]
        value.list.values.extend(values)
    elif isinstance(val, dict):
        for k, v in val.items():
            value.map.values[k].CopyFrom(make_value(v))
    return value

def make_index_config_list(config):
    return [make_index_config(c) for c in config]

def make_index_config(ixcfg):
    index_config = apollo.IndexConfig()
    index_config.column = ixcfg['column']
    if 'index_options' in ixcfg:
        opts = ixcfg['index_options']
        index_config.options.char_filter = opts['char_filter'].value
        index_config.options.tokenizer = opts['tokenizer'].value
        tf = opts['token_filter']
        index_config.options.token_filter.transform = tf['transform'].value
        index_config.options.token_filter.add.extend(tf['add'])
        index_config.options.token_filter.delete.extend(tf['delete'])
        index_config.options.token_filter.stats = tf['stats'].value
    return index_config

def make_posting_filter(filter):
    pf = apollo.PostingFilter()
    pf.sort_by = filter['sort_by'].value
    # empty bytes denote nil. proto3 defaults to 0 for numeric values
    # thus, we use bytes to distinguish if 0 is explictly set.
    pf.start_ts = uIntToBinaryDefault(filter.get('start_ts', False))
    pf.end_ts = uIntToBinaryDefault(filter.get('end_ts', False))
    pf.max_postings = filter['max_postings']
    return pf

def make_update_operation_list(uolist):
    return [make_update_operation(op) for op in uolist]

def make_update_operation(op):
    update_operation = apollo.UpdateOperation()
    update_operation.field = op['field']
    update_instruction = op.get('updateInstruction', {
            'instruction': enum.Instruction.overwrite
            })
    update_operation.update_instruction.instruction = update_instruction['instruction'].value
    update_operation.update_instruction.threshold = uIntToBinaryDefault(update_instruction.get('threshold', False))
    update_operation.update_instruction.set_value = uIntToBinaryDefault(update_instruction.get('set_value', False))
    update_operation.value.int = op['value']
    update_operation.default_value.int = op.get('default_value', 0)
    return update_operation

def uIntToBinaryDefault(uint):
    if uint:
        byte_len = (uint.bit_length() + 7) // 8
        return uint.to_bytes(byte_len, byteorder='big')
    else:
        return b''

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
            return format_fields(response.columns.fields)
        elif response.HasField('key_columns_pair'):
            return format_kcp(response.key_columns_pair)
        elif response.HasField('key_columns_list'):
            return format_kcl(response.key_columns_list)
        elif response.HasField('proplist'):
            return format_fields(response.proplist.fields)
        elif response.HasField('kcp_it'):
            return format_kcp_it(response.kcp_it)
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

def format_fields(fields):
    return dict([format_field(f) for f in fields])

def format_field(field):
    name = field.name
    value = format_value(field.value)
    return (name, value)

def format_value(val):
    value = None
    if val.HasField('boolean'):
        value = val.boolean
    elif val.HasField('int'):
        value = val.int
    elif val.HasField('binary'):
        value = val.binary
    elif val.HasField('null'):
        value == None
    elif val.HasField('double'):
        value = val.double
    elif val.HasField('string'):
        value = val.string
    elif val.HasField('list'):
        value = [format_value(v) for v in val.list.values]
    elif val.HasField('map'):
        value = {k: format_value(v) for k, v in val.map.values.items()}
    return value

def format_kcp(kcp):
    return (format_fields(kcp.key), format_fields(kcp.columns))

def format_kcp_it(kcp_it):
    return {'kcp': format_kcp(kcp_it.key_columns_pair),
            'it': kcp_it.it}

def format_continuation(cont):
    if cont.complete:
        return 'complete'
    else:
        return format_fields(cont.key)

def format_kcl(key_columns_list):
    kcl = [format_kcp(kcp) for kcp in key_columns_list.list]
    cont = format_continuation(key_columns_list.continuation)
    return {'key_columns_list': kcl, 'continuation': cont}

def format_postings(postings):
    return [format_posting(p) for p in postings.list]

def format_posting(posting):
    return {'key': format_fields(posting.key),
            'timestamp': posting.timestamp,
            'frequency': posting.frequency,
            'position': posting.position}
