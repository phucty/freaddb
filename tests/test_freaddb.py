import shutil

import marisa_trie
from tqdm import tqdm

from freaddb.db_lmdb import SIZE_1GB, DBSpec, FReadDB, ToBytes, profile


@profile
def test_db_basic():
    data_file = "/tmp/freaddb/db_test_basic"
    shutil.rmtree(data_file, ignore_errors=True)
    data_schema = [
        DBSpec(
            name="data0",
            integerkey=False,
            bytes_value=ToBytes.OBJ,
            compress_value=True,
        ),
        DBSpec(name="data1", integerkey=True, bytes_value=ToBytes.OBJ),
        DBSpec(name="data2", integerkey=False, bytes_value=ToBytes.PICKLE),
        DBSpec(name="data3", integerkey=False, bytes_value=ToBytes.BYTES),
        DBSpec(name="data4", integerkey=True, bytes_value=ToBytes.INT_NUMPY),
        DBSpec(name="data5", integerkey=True, bytes_value=ToBytes.INT_BITMAP),
        DBSpec(name="data6", combinekey=True),
        DBSpec(
            name="data7",
            combinekey=True,
            bytes_value=ToBytes.INT_NUMPY,
        ),
    ]
    data = {
        "data0": {"One": {1: "One"}, "Two": {2: "Two"}},
        "data1": {1: "One", 2: "Two"},
        "data2": {"One": 1, "Two": 2},
        "data3": {"One": b"1", "Two": b"2"},
        "data4": {i: list(range(i * 10)) for i in range(10, 20)},
        "data5": {i: list(range(i * 10)) for i in range(10, 20)},
        "data6": {(1, 2): "One", (2, 3): "Two"},
        "data7": {
            (1, 0, 1): [4],
            (1, 1, 1): [4],
            (1, 2, 3): [2, 2],
            (1, 2, 4): [3],
            (1, 2, 5): [1],
            (8535637, 1, 2): [3],
            (13699655, 1, 2): [3],
        },
    }
    to_list_data = {"data4", "data5"}

    db = FReadDB(db_file=data_file, db_schema=data_schema, buff_limit=SIZE_1GB)
    for data_name, data_items in data.items():
        for key, value in data_items.items():
            db.add_buff(data_name, key, value)
    db.save_buff()
    db.compress()
    db.close()
    db = FReadDB(db_file=data_file, readonly=True)

    # Get a key
    sample = db.get_value("data1", 1)
    assert sample == "One"

    # Get many keys
    for data_name, data_samples in data.items():
        sample = db.get_values(data_name, list(data_samples.keys()))
        if data_name in to_list_data:
            sample = {k: list(v) for k, v in sample.items()}
        assert set(sample) == set(data_samples)

    for k in db.head("data7", 20):
        print(f"{k}")

    print("Query: (1, 2)")
    for k, v in db.get_iter_with_prefix("data7", (1, 2)):
        print(f"{k}: {v}")

    print("Query: (1, )")
    for k, v in db.get_iter_with_prefix("data7", (1,)):
        print(f"{k}: {v}")

    print("Query: (1, 0, 1)")
    for k, v in db.get_iter_with_prefix("data7", (1, 0, 1)):
        print(f"{k}: {v}")

    print("Query: (2, )")
    for k, v in db.get_iter_with_prefix("data7", (2,)):
        print(f"{k}: {v}")

    print("Query: (13699655, )")
    for k, v in db.get_iter_with_prefix("data7", (13699655,)):
        print(f"{k}: {v}")


@profile
def test_db_basic_split_databases():
    data_file = "/tmp/freaddb/db_test_basic_split"
    shutil.rmtree(data_file, ignore_errors=True)
    data_schema = [
        DBSpec(
            name="data0",
            integerkey=False,
            bytes_value=ToBytes.OBJ,
            compress_value=True,
        ),
        DBSpec(name="data1", integerkey=True, bytes_value=ToBytes.OBJ),
        DBSpec(name="data2", integerkey=False, bytes_value=ToBytes.PICKLE),
        DBSpec(name="data3", integerkey=False, bytes_value=ToBytes.BYTES),
        DBSpec(name="data4", integerkey=True, bytes_value=ToBytes.INT_NUMPY),
        DBSpec(name="data5", integerkey=True, bytes_value=ToBytes.INT_BITMAP),
        DBSpec(name="data6", integerkey=True, combinekey=True),
        DBSpec(name="data7", integerkey=True, combinekey=True),
    ]
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

    db = FReadDB(
        db_file=data_file,
        db_schema=data_schema,
        buff_limit=SIZE_1GB,
        split_subdatabases=True,
    )
    for data_name, data_items in data.items():
        for key, value in data_items.items():
            db.add_buff(data_name, key, value)
    db.save_buff()
    db.compress()
    db.close()
    db = FReadDB(db_file=data_file, readonly=True)

    # Get a key
    sample = db.get_value("data1", 1)
    assert sample == "One"

    # Get many keys
    for data_name, data_samples in data.items():
        sample = db.get_values(data_name, list(data_samples.keys()))
        if data_name in to_list_data:
            sample = {k: list(v) for k, v in sample.items()}
        assert sample == data_samples


@profile
def test_db_large():
    data_file = "/tmp/freaddb/db_test_large"
    shutil.rmtree(data_file, ignore_errors=True)
    data_schema = [
        DBSpec(
            name="data0",
            integerkey=True,
            bytes_value=ToBytes.INT_BITMAP,
        ),
        DBSpec(
            name="data1",
            integerkey=False,
            bytes_value=ToBytes.OBJ,
            compress_value=True,
        ),
    ]

    limit = 1_000

    data = {
        "data0": {i: list(range(100)) for i in range(limit)},
        "data1": {str(i): str(list(range(100))) for i in range(limit)},
    }
    to_list_data = {"data0"}

    db = FReadDB(db_file=data_file, db_schema=data_schema, buff_limit=SIZE_1GB)
    for data_name, data_items in data.items():
        for key, value in data_items.items():
            db.add_buff(data_name, key, value)
    db.save_buff()
    db.compress()
    db.close()
    db = FReadDB(db_file=data_file, readonly=True)
    for data_name, data_samples in data.items():
        for key, value in data_samples.items():
            sample = db.get_value(data_name, key)
            if data_name in to_list_data:
                sample = list(sample)
            assert len(sample) == len(value)


@profile
def test_db_large_split():
    data_file = "/tmp/freaddb/db_test_large_split"
    shutil.rmtree(data_file, ignore_errors=True)
    data_schema = [
        DBSpec(
            name="data0",
            integerkey=True,
            bytes_value=ToBytes.INT_BITMAP,
        ),
        DBSpec(
            name="data1",
            integerkey=False,
            bytes_value=ToBytes.OBJ,
            compress_value=True,
        ),
    ]

    limit = 1_000

    data = {
        "data0": {i: list(range(100)) for i in range(limit)},
        "data1": {str(i): str(list(range(100))) for i in range(limit)},
    }
    to_list_data = {"data0"}

    db = FReadDB(
        db_file=data_file,
        db_schema=data_schema,
        buff_limit=SIZE_1GB,
        split_subdatabases=True,
    )
    for data_name, data_items in data.items():
        for key, value in data_items.items():
            db.add_buff(data_name, key, value, is_serialize_value=False)

    db.save_buff()
    db.compress()
    db.close()
    db = FReadDB(db_file=data_file, readonly=True)
    for data_name, data_samples in data.items():
        for key, value in data_samples.items():
            sample = db.get_value(data_name, key)
            if data_name in to_list_data:
                sample = list(sample)
            assert len(sample) == len(value)


@profile
def test_db_large_qid_split():
    data_file = "/tmp/freaddb/db_test_large_qid_split"
    shutil.rmtree(data_file, ignore_errors=True)
    data_schema = [
        DBSpec(name="qid_lid"),
        DBSpec(name="lid_qid", integerkey=True),
    ]

    limit = 1_000

    db = FReadDB(
        db_file=data_file,
        db_schema=data_schema,
        buff_limit=SIZE_1GB,
        split_subdatabases=True,
    )
    for i in tqdm(range(limit), total=limit):
        db.add_buff("qid_lid", f"Q{i + 1}", i, is_serialize_value=False)
        db.add_buff("lid_qid", i, f"Q{i + 1}", is_serialize_value=False)

    db.save_buff()
    db.compress()
    db.close()
