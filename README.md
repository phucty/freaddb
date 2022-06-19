# FReadDB

FReadDB: Fast Read Database is implemented with LMDB (key-value database) as the underlying storage. We use this DB as a data storage, and RAM of [MTab system](https://mtab.kgraph.jp). 

## Installation

```bash
pip install freaddb
```

## Usage

```python
from freaddb import config
from freaddb.db_lmdb import DBSpec, FReadDB

# Data file directory
data_file = "/tmp/freaddb/db_test_basic"

# Define sub database schema
# keys are strings, values are python objs and compress values 
db_schema_0 = DBSpec(name="data0", integerkey=False, bytes_value=config.ToBytesType.OBJ, compress_value=True)

# key are integers, values are python objects serialized with msgpack and no compress values
db_schema_1 = DBSpec(name="data1", integerkey=True, bytes_value=config.ToBytesType.OBJ)

# key are strings, values are python objects serialized with pickle
db_schema_2 = DBSpec(name="data2", integerkey=False, bytes_value=config.ToBytesType.PICKLE)

# key are strings, values are bytes
db_schema_3 = DBSpec(name="data3", integerkey=False, bytes_value=config.ToBytesType.BYTES)

# key are integers, values are list integers serialized with numpy
db_schema_4 = DBSpec(name="data4", integerkey=False, bytes_value=config.ToBytesType.INT_NUMPY)

# key are integers, values are list integers serialized with BITMAP
db_schema_5 = DBSpec(name="data5", integerkey=False, bytes_value=config.ToBytesType.INT_BITMAP)

# Add 6 sub databases
data_schema = [db_schema_0, db_schema_1, db_schema_2, db_schema_3, db_schema_4, db_schema_5]


# Example data
data = {
        "data0": {"One": {1: "One"}, "Two": {2: "Two"}},
        "data1": {1: "One", 2: "Two"},
        "data2": {"One": 1, "Two": 2},
        "data3": {"One": b"1", "Two": b"2"},
        "data4": {i: list(range(i * 10)) for i in range(10, 20)},
        "data5": {i: list(range(i * 10)) for i in range(10, 20)},
    }
to_list_data = {"data4", "data5"}

# Create data with data_file, data_schema, and buffer is 1GB 
db = FReadDB(db_file=data_file, db_schema=data_schema, buff_limit=config.SIZE_1GB)

# Add data to FReadDB
for data_name, data_items in data.items():
    for key, value in data_items.items():
        db.add(data_name, key, value)
        
# Make sure save all buffer to disk
db.save_buff()

# Access data
sample = db.get_value("data1", 1)
assert sample == "One"

for data_name, data_samples in data.items():
    sample = db.get_values(data_name, list(data_samples.keys()))
    if data_name in to_list_data:
        sample = {k: list(v) for k, v in sample.items()}
    assert sample == data_samples

```
