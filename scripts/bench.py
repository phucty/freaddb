import gc
import pickle
import shutil
from xmlrpc.client import FastParser

import marisa_trie
import pycedar
from tqdm import tqdm

from freaddb.db_lmdb import SIZE_1GB, DBSpec, FReadDB, ToBytes, profile


@profile
def dict_create_save(data_file: str, limit: int):
    db = [
        dict(),
        dict(),
    ]
    for i in range(limit):
        db[0][f"Q{i + 1}"] = i
        db[1][i] = f"Q{i + 1}"
    with open(data_file + ".pkl", "wb") as f:
        pickle.dump(db, f)
    del db


@profile
def dict_retrieval_single(data_file, queries):
    gc.collect()
    with open(data_file + ".pkl", "rb") as f:
        lmdb = pickle.load(f)

    for query in queries:
        item = lmdb[0].get(query)
        if item is None:
            continue

        assert lmdb[1].get(item) == query


@profile
def trie_create_save(data_file: str, limit: int):
    qid_list = marisa_trie.Trie([f"Q{i + 1}" for i in range(limit)])
    qid_list.save(data_file)
    return True


# @profile
# def rtrie_create_save(data_file: str, limit: int):
#     keys = [f"Q{i + 1}" for i in range(limit)]
#     values = [tuple([i]) for i in range(limit)]
#     qid_list = marisa_trie.RecordTrie("I", zip(keys, values))
#     qid_list.save(data_file)
#     return True


# @profile
# def rtrie_load(data_file: str):
#     qid_list = marisa_trie.RecordTrie("I")
#     qid_list.load(data_file)
#     return qid_list


@profile
def trie_retrieval(data_file, queries):
    gc.collect()
    trie = marisa_trie.Trie()
    trie.load(data_file)

    for query in queries:
        item = trie.get(query)
        if item is None:
            continue

        assert trie.restore_key(item) == query


@profile
def lmdb_create_save(data_file: str, limit: int):
    data_schema = [
        DBSpec(name="qid_lid"),
        DBSpec(name="lid_qid", integerkey=True),
    ]
    db = FReadDB(
        db_file=data_file,
        db_schema=data_schema,
        buff_limit=SIZE_1GB,
        split_subdatabases=True,
    )
    for i in range(limit):
        db.add_buff("qid_lid", f"Q{i + 1}", i, is_serialize_value=False)
        db.add_buff("lid_qid", i, f"Q{i + 1}", is_serialize_value=False)

    db.save_buff()
    db.compress(print_status=False)
    db.close()


@profile
def lmdb_retrieval_single(data_file, queries):
    gc.collect()
    lmdb = FReadDB(db_file=data_file, readonly=True)
    for query in queries:
        item = lmdb.get_value("qid_lid", query)
        if item is None:
            continue
        assert lmdb.get_value("lid_qid", item) == query


@profile
def lmdb_retrieval_multi(data_file, queries):
    gc.collect()
    lmdb = FReadDB(db_file=data_file, readonly=True)
    lids = lmdb.get_values("qid_lid", queries)
    lmdb.get_values("lid_qid", lids)


@profile
def cedar_create_save(data_file: str, limit: int):
    d_trie = pycedar.dict()
    for i in range(limit):
        d_trie.set(f"Q{i + 1}", i)
    d_trie.save(data_file)
    return True


@profile
def cedar_load(data_file: str):
    d_trie = pycedar.dict()
    d_trie.load(data_file)
    return d_trie


@profile
def cedar_retrieval_single(trie, queries):
    lids = {}
    for query in queries:
        item = trie.get(query)
        if item is None or (isinstance(item, int) and item < 0):
            continue

        lids[item] = query

    for lid, query in lids.items():
        item = trie.find_values(lid)
        for i in item:
            assert i == query
            break


@profile
def cedar_retrieval_multi(trie, queries):
    lids = {}
    for query in queries:
        item = trie.get(query)
        if item is None:
            continue

        lids[item] = query

    for lid, query in lids.items():
        item = trie.restore_key(lid)
        assert item == query


def bench_trie_vs_lmdb(limit=100_000):
    data_file = "/tmp/freaddb/db_test_large_qid_split_1"
    data_file_trie = "/tmp/freaddb/db_test_large_qid_split_2.trie"
    shutil.rmtree(data_file, ignore_errors=True)
    shutil.rmtree(data_file_trie, ignore_errors=True)

    queries = [f"Q{i}" for i in range(limit)]  #  if i % 10 == 1
    dict_create_save(data_file, limit)
    dict_retrieval_single(data_file, queries)
    #
    # Test with lmdb
    lmdb_create_save(data_file, limit)
    lmdb_retrieval_single(data_file, queries)
    lmdb_retrieval_multi(data_file, queries)

    # Test Tries
    trie_create_save(data_file_trie, limit)
    trie_retrieval(data_file_trie, queries)
    # del qid_list

    # cedar_create_save(data_file_trie, limit)
    # qid_list = cedar_load(data_file_trie)
    # cedar_retrieval_single(qid_list, queries)

    # Test Record Trie
    # not work
    # rtrie_create_save(data_file_trie, limit)
    # rqid_list = rtrie_load(data_file_trie)
    # trie_retrieval(rqid_list, queries)


if __name__ == "__main__":
    bench_trie_vs_lmdb(limit=1_000_000)
    # import dawg
    # dawg.IntDAWG({'foo': 1, 'bar': 2, 'foobar': 3})
    """
    100%|████████████████████████████████████████████████████████████████████████████████████| 1_000_000/1_000_000 [00:03<00:00, 308873.83it/s]
    qid_lid : 97.95% - 21.2MiB/1.0GiB
    lid_qid : 97.94% - 21.3MiB/1.0GiB
    Compressed: 97.95% - 42.5MiB/2.0GiB
    lmdb_create_save        Time: 0:00:12.703496    RSS: 90.2MiB    VMS: 575.7MiB
    lmdb_load       Time: 0:00:00.002766    RSS: 24.0KiB    VMS: 42.5MiB
    lmdb_retrieval_single   Time: 0:00:05.498542    RSS: 42.6MiB    VMS: 0B
    lmdb_retrieval_multi    Time: 0:00:04.404589    RSS: 28.9MiB    VMS: 26.0MiB
    trie_create_save        Time: 0:00:00.846476    RSS: 55.0MiB    VMS: 30.8MiB
    trie_load       Time: 0:00:00.000551    RSS: 8.0KiB     VMS: 0B
    trie_retrieval  Time: 0:00:00.650987    RSS: 424.0KiB   VMS: 0B

    trie_create_save        Time: 0:00:00.912360    RSS: 77.6MiB    VMS: 39.9MiB
    trie_load       Time: 0:00:00.001336    RSS: 1.5MiB     VMS: 0B
    trie_retrieval  Time: 0:00:00.139793    RSS: 1.4MiB     VMS: 0B
    100%|████████████████████████████████████████████████████████████████████████████████████| 1000000/1000000 [00:03<00:00, 287676.39it/s]
    qid_lid : 97.95% - 21.2MiB/1.0GiB
    lid_qid : 97.94% - 21.3MiB/1.0GiB
    Compressed: 97.95% - 42.5MiB/2.0GiB
    lmdb_create_save        Time: 0:00:10.928423    RSS: 55.5MiB    VMS: 26.7MiB
    lmdb_load       Time: 0:00:00.001359    RSS: 24.0KiB    VMS: 42.5MiB
    lmdb_retrieval_single   Time: 0:00:01.310285    RSS: 42.5MiB    VMS: 0B
    lmdb_retrieval_multi    Time: 0:00:00.848976    RSS: 5.6MiB     VMS: 2.0MiB

    trie_create_save        Time: 0:01:31.817849    RSS: 905.5MiB   VMS: 1.1GiB
    trie_load       Time: 0:00:00.042253    RSS: 36.0KiB    VMS: 0B
    trie_retrieval  Time: 0:00:14.964123    RSS: 36.9MiB    VMS: 79.0MiB
    100%|████████████████████████████████████████████████████████████████████████████████| 100000000/100000000 [05:56<00:00, 280513.48it/s]
    qid_lid : 24.64% - 2.3GiB/3.0GiB
    lid_qid : 24.81% - 2.3GiB/3.0GiB
    Compressed: 24.72% - 4.5GiB/6.0GiB
    lmdb_create_save        Time: 0:42:13.980900    RSS: 1.4GiB     VMS: 9.2MiB
    lmdb_load       Time: 0:00:00.005170    RSS: 536.0KiB   VMS: 4.5GiB
    lmdb_retrieval_single   Time: 0:02:38.491916    RSS: 5.2GiB     VMS: 0B
    lmdb_retrieval_multi    Time: 0:01:15.016830    RSS: 581.2MiB   VMS: 260.0MiB
    --> Using trie to store 
    """
