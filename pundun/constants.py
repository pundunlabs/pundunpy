from enum import Enum

class CharFilter(Enum):
    nfc = 0
    nfd = 1
    nfkc = 2
    nfkd = 2

class Comparator(Enum):
    descending = 0
    ascending = 1

class DataModel(Enum):
    kv = 0
    array = 1
    map = 2

class HashingMethod(Enum):
    virtualnodes = 0
    consistent = 1
    uniform = 2
    rendezvous = 3

class SortBy(Enum):
    relevance = 0
    timestamp = 1

class TimeUnit(Enum):
    second =  0
    millisecond = 1
    microsecond = 2
    nanosecond = 3

class Tokenizer(Enum):
  unicode_word_boundaries = 0

class TokenStats(Enum):
    nostats = 0
    unique = 1
    frequency = 2
    position = 3

class TokenTransform(Enum):
    lowercase = 0
    uppercase = 1
    casefold = 2

class Type(Enum):
    rocksdb = 6

class Instruction(Enum):
    increment = 0
    overwirete = 7
