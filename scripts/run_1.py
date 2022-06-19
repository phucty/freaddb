from freaddb.db_lmdb import FReadDB, DBSpec
from freaddb import config

if __name__ == "__main__":
    data_file = "/tmp/freaddb/db_test_1"
    # data_schema = [
    #     DBSpec(name="data1", integerkey=True),
    #     DBSpec(name="data2", integerkey=False, bytes_value=config.ToBytesType.PICKLE),
    #     DBSpec(name="data3", integerkey=False, bytes_value=config.ToBytesType.BYTES),
    # ]
    # data = {
    #     "data1": [[1, "Một"], [2, "Hai"]],
    #     "data2": [["Một", 1], ["Hai", 2]],
    #     "data3": [["Một", b"1"], ["Hai", b"2"]],
    # }
    # db = FReadDB(data_file, data_schema)
    #
    # for data_name, data_items in data.items():
    #     for key, value in data_items:
    #         db.add(data_name, key, value)
    # db.save_buff()

    db = FReadDB(data_file)
    sample_1 = db.get_value("data1", [1, 2])
    sample_2 = db.get_value("data2", ["Một", "Hai"])
    sample_3 = db.get_value("data3", ["Một", "Hai"])
    print(sample_1)
    print(sample_2)
    print(sample_3)
    print("Done")
