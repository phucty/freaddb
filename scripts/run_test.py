import json
import shutil

from freaddb.db_lmdb import DBSpec, FReadDB, ToBytes, profile
from scripts import bench
from tests import test_freaddb


@profile
def run_test():
    data_file = "/tmp/freaddb/db_test_1"
    data_schema = [
        DBSpec(name="data1", integerkey=True),
        DBSpec(name="data2", integerkey=False, bytes_value=ToBytes.PICKLE),
        DBSpec(name="data3", integerkey=False, bytes_value=ToBytes.BYTES),
        DBSpec(name="data4", integerkey=True, combinekey=True),
    ]
    data = {
        "data1": [[1, "Một"], [2, "Hai"]],
        "data2": [["Một", 1], ["Hai", 2]],
        "data3": [["Một", b"1"], ["Hai", b"2"]],
        "data4": [[(1, 2), "Một"], [(3, 4), "Hai"]],
    }
    db = FReadDB(data_file, data_schema, split_subdatabases=True)

    for data_name, data_items in data.items():
        for key, value in data_items:
            db.add_buff(data_name, key, value)
    db.save_buff()
    db.compress_subdatabase("data1")
    db.compress()
    db.close()
    db = FReadDB(db_file=data_file, readonly=True)

    print(db.get_value("data1", 1))
    print(db.get_value("data4", (1, 2)))
    print(db.get_values("data1", [1, 2]))
    print(db.get_values("data2", ["Một", "Hai"]))
    print(db.get_values("data3", ["Một", "Hai"]))
    print("Done")
    db.close()


@profile
def run_readme_example():
    from freaddb.db_lmdb import SIZE_1GB, DBSpec, FReadDB, ToBytes

    # Data file directory
    data_file = "/tmp/freaddb/db_test_basic"
    # Clear old data
    shutil.rmtree(data_file, ignore_errors=True)

    # Define sub database schema
    data_schema = [
        # keys are strings, values are python objs and compress values
        DBSpec(
            name="data0",
            integerkey=False,
            bytes_value=ToBytes.OBJ,
            compress_value=True,
        ),
        # key are integers, values are python objects serialized with msgpack and no compress values
        DBSpec(name="data1", integerkey=True, bytes_value=ToBytes.OBJ),
        # key are strings, values are python objects serialized with pickle
        DBSpec(name="data2", integerkey=False, bytes_value=ToBytes.PICKLE),
        # key are strings, values are bytes
        DBSpec(name="data3", integerkey=False, bytes_value=ToBytes.BYTES),
        # key are integers, values are list integers serialized with numpy
        DBSpec(name="data4", integerkey=True, bytes_value=ToBytes.INT_NUMPY),
        # key are integers, values are list integers serialized with BITMAP
        DBSpec(name="data5", integerkey=True, bytes_value=ToBytes.INT_BITMAP),
        # key are combination of two integers
        DBSpec(name="data6", combinekey=True),
        # key are combination of three integers
        DBSpec(name="data7", combinekey=True),
    ]

    # Example data
    data = {
        "data0": {"One": {1: "One"}, "Two": {2: "Two"}},
        "data1": {1: "One", 2: "Two"},
        "data2": {"One": 1, "Two": 2},
        "data3": {"One": b"1", "Two": b"2"},
        "data4": {i: list(range(i * 10)) for i in range(10, 20)},
        "data5": {i: list(range(i * 10)) for i in range(10, 20)},
        "data6": {(1, 2): "One", (2, 3): "Two"},
        "data7": {(1, 2, 3): "One", (2, 3, 4): "Two"},
    }
    to_list_data = {"data4", "data5"}

    # Create data with data_file, data_schema, and buffer is 1GB
    db = FReadDB(db_file=data_file, db_schema=data_schema, buff_limit=SIZE_1GB)

    # Add data to FReadDB
    for data_name, data_items in data.items():
        for key, value in data_items.items():
            db.add_buff(data_name, key, value)
    # db.delete_buff("data0", "One")

    # Make sure save all buffer to disk
    db.save_buff()

    ####################################################
    # (Optional for readonly database) Compress database
    db.compress()
    db.close()
    db = FReadDB(db_file=data_file, readonly=True)
    ####################################################

    # Access data
    # Get a key
    sample = db.get_value("data6", (1, 2))
    assert sample == "One"

    sample = db.get_value("data7", (1, 2, 3))
    assert sample == "One"

    sample = db.get_value("data1", 1)
    assert sample == "One"

    for data_name, data_samples in data.items():
        sample = db.get_values(data_name, list(data_samples.keys()))
        if data_name in to_list_data:
            sample = {k: list(v) for k, v in sample.items()}
        assert sample == data_samples

    print(json.dumps(db.stats(), indent=2))


if __name__ == "__main__":
    run_readme_example()
    # run_test()
    # test_freaddb.test_db_basic()
    # test_freaddb.test_db_basic_split_databases()
    # test_freaddb.test_db_large()
    # test_freaddb.test_db_large_split()
    # test_freaddb.test_db_large_qid_split()
    # bench.bench_trie_vs_lmdb(limit=100_000_0)
