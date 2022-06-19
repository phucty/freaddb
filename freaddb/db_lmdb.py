import gc
import os
import random
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import List, Optional

import lmdb
import numpy
from tqdm import tqdm

from freaddb import config, io_worker, utils


@dataclass
class DBSpec:
    name: str
    integerkey: bool = False
    is_64bit: bool = False
    bytes_value: bool = config.ToBytesType.OBJ
    compress_value: bool = False


class FReadDB:
    def __init__(
        self,
        db_file: str,
        db_schema: Optional[List[DBSpec]] = None,
        map_size: int = config.LMDB_MAP_SIZE,
        readonly: bool = False,
        buff_limit: int = config.LMDB_BUFF_LIMIT,
    ):
        self.metadata_file = db_file + ".json"
        self.db_file = db_file
        io_worker.create_dir(self.metadata_file)
        io_worker.create_dir(self.db_file)
        if db_schema:
            self.db_schema = {db_spec.name: db_spec for db_spec in db_schema}
            self.buff_limit = buff_limit
            self.save_metadata_info(db_schema, buff_limit)
        else:
            self.db_schema, self.buff_limit = self.load_metadata_info()

        self.max_db = len(self.db_schema)
        self.map_size = map_size
        self.readonly = readonly
        self.env = lmdb.open(
            self.db_file,
            map_async=True,
            map_size=self.map_size,
            subdir=False,
            lock=False,
            max_dbs=self.max_db,
            readonly=self.readonly,
        )
        self.dbs = self.init_sub_databases()

        self.buff = defaultdict(list)
        self.buff_size = 0

    def save_metadata_info(self, db_schema, buff_limit):
        json_obj = {
            "db_schema": [asdict(db_i) for db_i in db_schema],
            "buff_limit": buff_limit,
        }
        io_worker.save_json_file(self.metadata_file, json_obj)

    def load_metadata_info(self):
        json_obj = io_worker.read_json_file(self.metadata_file)
        db_schema = {obj["name"]: DBSpec(**obj) for obj in json_obj["db_schema"]}
        buff_limit = json_obj["buff_limit"]
        return db_schema, buff_limit

    def init_sub_databases(self):
        db_dict = {}
        for db_spec in self.db_schema.values():
            db_dict[db_spec.name] = self.env.open_db(
                db_spec.name.encode(config.ENCODING), integerkey=db_spec.integerkey
            )
        return db_dict

    def get_map_size(self):
        tmp = self.env.info().get("map_size")
        if not tmp:
            return "Unknown"
        return f"{tmp / config.SIZE_1GB:.0f}GB"

    def close(self):
        self.env.close()

    def compress(self):
        """
        Copy current env to new one (reduce file size)
        :return:
        :rtype:
        """
        print(self.env.stat())
        if self.env.stat().get("map_size"):
            print("%.2fGB" % (self.env.stat()["map_size"] % config.SIZE_1GB))
        new_dir = self.db_file + ".copy"
        self.env.copy(path=new_dir, compact=True)
        try:
            if os.path.exists(self.db_file):
                os.remove(self.db_file)
        except Exception as message:
            print(message)
        os.rename(new_dir, self.db_file)

    def get_random_key(self, db_name):
        db = self.dbs[db_name]
        integerkey = self.db_schema[db_name].integerkey
        is_64bit = self.db_schema[db_name].is_64bit
        with self.env.begin(db=db, write=False) as txn:
            random_index = random.randint(0, self.get_db_size(db))
            cur = txn.cursor()
            cur.first()
            key = utils.deserialize_key(
                cur.key(), integerkey=integerkey, is_64bit=is_64bit
            )
            for i, k in enumerate(cur.iternext(values=False)):
                if i == random_index:
                    key = utils.deserialize_key(k, integerkey=integerkey)
                    break
        return key

    def get_iter_integerkey(self, db_name, from_i=0, to_i=-1, get_values=True):
        db = self.dbs[db_name]
        bytes_value = self.db_schema[db_name].bytes_value
        compress_value = self.db_schema[db_name].compress_value
        is_64bit = self.db_schema[db_name].is_64bit

        with self.env.begin(db=db, write=False) as txn:
            if to_i == -1:
                to_i = self.get_db_size(db)
            cur = txn.cursor()
            cur.set_range(
                utils.serialize_key(from_i, integerkey=True, is_64bit=is_64bit)
            )
            for item in cur.iternext(values=get_values):
                if get_values:
                    key, value = item
                else:
                    key = item
                key = utils.deserialize_key(key, integerkey=True, is_64bit=is_64bit)
                if key > to_i:
                    break
                if get_values:
                    value = utils.deserialize_value(
                        value, bytes_value=bytes_value, compress_value=compress_value,
                    )
                    yield key, value
                else:
                    yield key
            cur.next()

    def get_iter_with_prefix(self, db_name, prefix, get_values=True):
        db = self.dbs[db_name]
        bytes_value = self.db_schema[db_name].bytes_value
        compress_value = self.db_schema[db_name].compress_value
        integerkey = self.db_schema[db_name].integerkey
        is_64bit = self.db_schema[db_name].is_64bit

        with self.env.begin(db=db, write=False) as txn:
            cur = txn.cursor()
            prefix = utils.serialize_key(
                prefix, integerkey=integerkey, is_64bit=is_64bit
            )
            cur.set_range(prefix)

            while cur.key().startswith(prefix) is True:
                try:
                    if cur.key() and not cur.key().startswith(prefix):
                        continue
                    key = utils.deserialize_key(
                        cur.key(), integerkey=integerkey, is_64bit=is_64bit
                    )
                    if get_values:
                        value = utils.deserialize_value(
                            cur.value(),
                            bytes_value=bytes_value,
                            compress_value=compress_value,
                        )
                        yield key, value
                    else:
                        yield key
                except Exception as message:
                    print(message)
                cur.next()

    def is_available(self, db_name, key_obj):
        db = self.dbs[db_name]
        integerkey = self.db_schema[db_name].integerkey
        is_64bit = self.db_schema[db_name].is_64bit
        with self.env.begin(db=db) as txn:
            key_obj = utils.serialize_key(
                key_obj, integerkey=integerkey, is_64bit=is_64bit
            )
            if key_obj:
                try:
                    value_obj = txn.get(key_obj)
                    if value_obj:
                        return True
                except Exception as message:
                    print(message)
        return False

    def get_memory_size(self, db_name, key_obj):
        db = self.dbs[db_name]
        integerkey = self.db_schema[db_name].integerkey
        is_64bit = self.db_schema[db_name].is_64bit
        with self.env.begin(db=db, buffers=True) as txn:
            key_obj = utils.serialize_key(
                key_obj, integerkey=integerkey, is_64bit=is_64bit
            )
            responds = None
            if key_obj:
                try:
                    value_obj = txn.get(key_obj)
                    if value_obj:
                        return len(value_obj)
                except Exception as message:
                    print(message)

            return responds

    def save_buff(self):
        for db_name, buff in self.buff.items():
            db = self.dbs[db_name]
            bytes_value = self.db_schema[db_name].bytes_value
            compress_value = self.db_schema[db_name].compress_value
            integerkey = self.db_schema[db_name].integerkey
            is_64bit = self.db_schema[db_name].is_64bit
            self.write(
                self.env,
                db,
                buff,
                integerkey=integerkey,
                is_64bit=is_64bit,
                bytes_value=bytes_value,
                compress_value=compress_value,
            )

    def add(self, db_name, key, value):
        bytes_value = self.db_schema[db_name].bytes_value
        compress_value = self.db_schema[db_name].compress_value

        value = utils.serialize_value(
            value, bytes_value=bytes_value, compress_value=compress_value,
        )
        self.buff_size += len(value)
        self.buff[db_name].append([key, value])
        if self.buff_size > self.buff_limit:
            self.save_buff()
            del self.buff
            gc.collect()
            self.buff = defaultdict(list)
            self.buff_size = 0

    def get_values(self, db_name, key_objs, get_deserialize=True):
        db = self.dbs[db_name]
        bytes_value = self.db_schema[db_name].bytes_value
        compress_value = self.db_schema[db_name].compress_value
        integerkey = self.db_schema[db_name].integerkey
        is_64bit = self.db_schema[db_name].is_64bit
        with self.env.begin(db=db, buffers=True) as txn:
            if isinstance(key_objs, numpy.ndarray):
                key_objs = key_objs.tolist()
            responds = dict()

            if not (
                isinstance(key_objs, list)
                or isinstance(key_objs, set)
                or isinstance(key_objs, tuple)
            ):
                return responds

            key_objs = [utils.serialize_key(k, integerkey=integerkey) for k in key_objs]
            for k, v in txn.cursor(db).getmulti(key_objs):
                if not v:
                    continue
                k = utils.deserialize_key(k, integerkey=integerkey, is_64bit=is_64bit)
                if get_deserialize:
                    try:
                        v = utils.deserialize_value(
                            v, bytes_value=bytes_value, compress_value=compress_value
                        )
                    except Exception as message:
                        print(message)
                responds[k] = v

        return responds

    def get_value(self, db_name, key_obj, get_deserialize=True):
        db = self.dbs[db_name]
        bytes_value = self.db_schema[db_name].bytes_value
        compress_value = self.db_schema[db_name].compress_value
        integerkey = self.db_schema[db_name].integerkey
        is_64bit = self.db_schema[db_name].is_64bit
        with self.env.begin(db=db, buffers=True) as txn:
            key_obj = utils.serialize_key(
                key_obj, integerkey=integerkey, is_64bit=is_64bit
            )
            responds = None
            if not key_obj:
                return responds
            try:
                value_obj = txn.get(key_obj)
                if not value_obj:
                    return responds
                responds = value_obj
                if get_deserialize:
                    responds = utils.deserialize_value(
                        value_obj,
                        bytes_value=bytes_value,
                        compress_value=compress_value,
                    )

            except Exception as message:
                print(message)

        return responds

    def head(
        self, db_name, n, from_i=0,
    ):
        respond = defaultdict()
        for i, (k, v) in enumerate(self.get_db_iter(db_name, from_i=from_i)):
            respond[k] = v
            if i == n - 1:
                break
        return respond

    def get_db_iter(
        self, db_name, get_values=True, deserialize_obj=True, from_i=0, to_i=-1,
    ):
        db = self.dbs[db_name]
        bytes_value = self.db_schema[db_name].bytes_value
        compress_value = self.db_schema[db_name].compress_value
        integerkey = self.db_schema[db_name].integerkey
        is_64bit = self.db_schema[db_name].is_64bit
        if to_i == -1:
            to_i = self.get_db_size(db)

        with self.env.begin(db=db) as txn:
            cur = txn.cursor()
            for i, db_obj in enumerate(cur.iternext(values=get_values)):
                if i < from_i:
                    continue
                if i >= to_i:
                    break

                if get_values:
                    key, value = db_obj
                else:
                    key = db_obj
                try:
                    if deserialize_obj:
                        key = utils.deserialize_key(
                            key, integerkey=integerkey, is_64bit=is_64bit
                        )
                        if get_values:
                            value = utils.deserialize_value(
                                value,
                                bytes_value=bytes_value,
                                compress_value=compress_value,
                            )
                    if get_values:
                        return_obj = (key, value)
                        yield return_obj
                    else:
                        yield key
                except UnicodeDecodeError:
                    print(f"UnicodeDecodeError: {i}")
                except Exception:
                    print(i)
                    raise Exception

    def get_db_size(self, db_name):
        db = self.dbs[db_name]
        with self.env.begin(db=db) as txn:
            return txn.stat()["entries"]

    def delete(self, db_name, key, with_prefix=False):
        db = self.dbs[db_name]
        integerkey = self.db_schema[db_name].integerkey
        is_64bit = self.db_schema[db_name].is_64bit
        if not (
            isinstance(key, list) or isinstance(key, set) or isinstance(key, tuple)
        ):
            key = [key]

        if with_prefix:
            true_key = set()
            for k in key:
                for tmp_k in self.get_iter_with_prefix(db_name, k, get_values=False):
                    true_key.add(tmp_k)
            if true_key:
                key = list(true_key)

        deleted_items = 0
        with self.env.begin(db=db, write=True, buffers=True) as txn:
            for k in key:
                try:
                    status = txn.delete(
                        utils.serialize_key(k, integerkey=integerkey, is_64bit=is_64bit)
                    )
                    if status:
                        deleted_items += 1
                except Exception as message:
                    print(message)
        return deleted_items

    @staticmethod
    def write(
        env,
        db,
        data,
        sort_key=True,
        integerkey=False,
        is_64bit=False,
        bytes_value=config.ToBytesType.OBJ,
        compress_value=False,
        one_sample_write=False,
    ):
        data = utils.preprocess_data_before_dump(
            data,
            bytes_value=bytes_value,
            integerkey=integerkey,
            is_64bit=is_64bit,
            compress_value=compress_value,
            sort_key=sort_key,
        )
        added_items = 0
        try:
            with env.begin(db=db, write=True, buffers=True) as txn:
                if not one_sample_write:
                    _, added_items = txn.cursor().putmulti(data)
                else:
                    for k, v in data:
                        txn.put(k, v)
                        added_items += 1
        except lmdb.MapFullError:
            curr_limit = env.info()["map_size"]
            new_limit = curr_limit + config.SIZE_1GB * 5
            env.set_mapsize(new_limit)
            return FReadDB.write(env, db, data, sort_key=False)
        except lmdb.BadValsizeError:
            print(lmdb.BadValsizeError)
        except lmdb.BadTxnError:
            if one_sample_write:
                return FReadDB.write(
                    env, db, data, sort_key=False, one_sample_write=True,
                )
        except Exception:
            raise Exception
        return added_items

    @staticmethod
    def write_with_buffer(
        env,
        db,
        data,
        sort_key=True,
        integerkey=False,
        is_64bit=False,
        bytes_value=config.ToBytesType.OBJ,
        compress_value=False,
        show_progress=True,
        step=10000,
        message="DB Write",
    ):
        data = utils.preprocess_data_before_dump(
            data,
            bytes_value=bytes_value,
            integerkey=integerkey,
            is_64bit=is_64bit,
            compress_value=compress_value,
            sort_key=sort_key,
        )

        def update_desc():
            return f"{message} buffer: {buff_size / config.LMDB_BUFF_LIMIT * 100:.0f}%"

        p_bar = None
        buff_size = 0
        i_pre = 0
        if show_progress:
            p_bar = tqdm(total=len(data))

        for i, (k, v) in enumerate(data):
            if show_progress and i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())
            buff_size += len(k) + len(v)

            if buff_size >= config.LMDB_BUFF_LIMIT:
                c = FReadDB.write(env, db, data[i_pre:i], sort_key=False)
                if c != len(data[i_pre:i]):
                    print(
                        f"WriteError: Missing data. Expected: {len(data[i_pre:i])} - Actual: {c}"
                    )
                i_pre = i
                buff_size = 0

        if buff_size:
            FReadDB.write(env, db, data[i_pre:], sort_key=False)

        if show_progress:
            p_bar.update(len(data) % step)
            p_bar.set_description(desc=update_desc())
            p_bar.close()

    def update_bulk_with_buffer(
        self,
        db_name,
        data,
        update_type=config.DBUpdateType.SET,
        show_progress=True,
        step=10000,
        message="",
        buff_limit=config.LMDB_BUFF_LIMIT,
    ):
        db = self.dbs[db_name]
        bytes_value = self.db_schema[db_name].bytes_value
        compress_value = self.db_schema[db_name].compress_value
        integerkey = self.db_schema[db_name].integerkey
        is_64bit = self.db_schema[db_name].is_64bit

        buff = []
        p_bar = None
        c_skip, c_update, c_new, c_buff = 0, 0, 0, 0

        def update_desc():
            return (
                f"{message}"
                f"|Skip:{c_skip:,}"
                f"|New:{c_new:,}"
                f"|Update:{c_update:,}"
                f"|Buff:{c_buff / buff_limit * 100:.0f}%"
            )

        if show_progress:
            p_bar = tqdm(total=len(data), desc=update_desc())

        for i, (k, v) in enumerate(data.items()):
            if show_progress and i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(update_desc())

            db_obj = self.get_value(db_name, k)
            if update_type == config.DBUpdateType.SET:
                if db_obj:
                    db_obj = set(db_obj)
                    v = set(v)
                    if db_obj and len(v) <= len(db_obj) and db_obj.issuperset(v):
                        c_skip += 1
                        continue
                    if db_obj:
                        v.update(db_obj)
                        c_update += 1
                    else:
                        c_new += 1
                else:
                    c_new += 1
            else:
                if db_obj:
                    v += db_obj
                    c_update += 1
                else:
                    c_new += 1

            k, v = utils.serialize(
                k,
                v,
                integerkey=integerkey,
                is_64bit=is_64bit,
                bytes_value=bytes_value,
                compress_value=compress_value,
            )

            c_buff += len(k) + len(v)
            buff.append((k, v))

            if c_buff >= buff_limit:
                FReadDB.write(self.env, db, buff)
                buff = []
                c_buff = 0

        if buff:
            FReadDB.write(self.env, db, buff)
        if show_progress:
            p_bar.set_description(desc=update_desc())
            p_bar.close()

    def drop_db(self, db):
        with self.env.begin(write=True) as in_txn:
            in_txn.drop(db)
            print(in_txn.stat())
