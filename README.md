# pundunpy
This is the Pundun DB Python Client.
### Dependencies

* python 3.5.3 (you can use `pyenv install 3.5.1`)
* scrampy
* protobuf
* google
See `requirements.txt` for python package requirements.

### Installation
pundunpy uses Pipfile and Python3.

You can easily use `pipenv` to do install all dependencies.

```
git clone https://github.com/falkevik/pundunpy
cd pundunpy
pipenv install
```
This should install all requirements.

### Usage
This guide assumes that you have installed and have a pundun instance running locally.

```
from pundun import Client
from pundun import utils
from pundun import constants as enum

client = Client('127.0.0.1', 8887, 'admin', 'secret')
```
First list all tables just to make sure the connection is working:
```
client.list_tables()
```

We can create a new table using
```
client.create_table('table_az', ['id', 'map'], {'num_of_shards': 1})
```
Reading and writing is taken care of by the `read/write` primitives:
```
mymap = {'a': 1, 'b': 1, 'c': 1}
mymap2 = {'a': 2, 'b': 2, 'c': 2}
key1 = {'id': 'same', 'map': mymap}
key2 = {'id': 'same', 'map': mymap2}
data1 = {'number': '1', 'text': 'One'}
data2 = {'number': '2', 'text': 'Two'}
client.write(table_name, key1
client.write(table_name, key2
client.read(table_name, key1)
client.read(table_name, key2)
```

We can read a range:
```
client.read_range(table_name, key2, key1, 2)
``

And we can delete the table using the `delete_table` method:
```
client.delete_table('table_az')
```

That's a small introduction, please look at the tests for more
information.
