import shutil

from freaddb import config
from freaddb.db_lmdb import DBSpec, FReadDB
from freaddb.utils import profile
from tests import test_freaddb


@profile
def run_test():
    data_file = "/tmp/freaddb/db_test_1"
    data_schema = [
        DBSpec(name="data1", integerkey=True),
        DBSpec(name="data2", integerkey=False, bytes_value=config.ToBytes.PICKLE),
        DBSpec(name="data3", integerkey=False, bytes_value=config.ToBytes.BYTES),
        DBSpec(name="data4", integerkey=True, combinekey=True),
    ]
    data = {
        "data1": [[1, "Một"], [2, "Hai"]],
        "data2": [["Một", 1], ["Hai", 2]],
        "data3": [["Một", b"1"], ["Hai", b"2"]],
        "data4": [[(1, 2), "Một"], [(3, 4), "Hai"]],
    }
    db = FReadDB(data_file, data_schema)

    for data_name, data_items in data.items():
        for key, value in data_items:
            db.add(data_name, key, value)
    db.save_buff()
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


if __name__ == "__main__":
    run_test()
    test_freaddb.test_db_basic()
    test_freaddb.test_db_basic_split_databases()
    test_freaddb.test_db_large()
    test_freaddb.test_db_large_split()
