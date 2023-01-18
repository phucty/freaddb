import dataclasses
import gc
import os
import random
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Iterator, List, Optional, Union

import lmdb
import numpy
from tqdm import tqdm

from freaddb import config, io_worker, utils


@dataclass
class DBSpec:
    name: str
    integerkey: bool = False
    is_64bit: bool = False
    bytes_value: bool = config.ToBytes.OBJ
    compress_value: bool = False
    combinekey: bool = False
    combinelen: int = 2


class FReadDB:
    def __init__(
        self,
        db_file: str,
        db_schema: Optional[List[DBSpec]] = None,
        map_size: int = None,
        readonly: bool = False,
        buff_limit: int = config.LMDB_BUFF_LIMIT,
        split_subdatabases=False,
    ):

        self.db_file = db_file
        io_worker.create_dir(self.db_file)

        self.metadata_file = db_file + ".json"
        io_worker.create_dir(self.metadata_file)

        if readonly and not os.path.exists(self.db_file) and not split_subdatabases:
            split_subdatabases = True

        if db_schema:
            self.db_schema = {db_spec.name: db_spec for db_spec in db_schema}
            self.buff_limit = buff_limit
            self.save_metadata_info(db_schema, buff_limit)
        else:
            self.db_schema, self.buff_limit = self.load_metadata_info()

        self.split_subdatabases = split_subdatabases
        self.max_db = len(self.db_schema)
        self.map_size = map_size
        self.readonly = readonly

        self.env, self.dbs = None, None
        self.init_env_and_sub_databases()

        self.buff = defaultdict(list)
        self.buff_size = 0

    def save_metadata_info(self, db_schema: List[DBSpec], buff_limit: int):
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

    def init_env_and_sub_databases(self) -> bool:
        self.env = {}
        self.dbs = {}
        default_env = None
        if not self.split_subdatabases:
            default_env = lmdb.open(
                self.db_file,
                map_async=True,
                writemap=True,
                subdir=False,
                lock=False,
                max_dbs=self.max_db,
                readonly=self.readonly,
            )

        for db_spec in self.db_schema.values():
            if self.split_subdatabases:
                self.env[db_spec.name] = lmdb.open(
                    self.db_file + f"_{db_spec.name}",
                    map_async=True,
                    writemap=True,
                    subdir=False,
                    lock=False,
                    max_dbs=2,
                    readonly=self.readonly,
                )

            else:
                self.env[db_spec.name] = default_env

            if self.map_size:
                if self.split_subdatabases:
                    self.env[db_spec.name].set_mapsize(
                        self.map_size // len(self.db_schema)
                    )
                else:
                    self.env[db_spec.name].set_mapsize(self.map_size)

            self.dbs[db_spec.name] = self.env[db_spec.name].open_db(
                db_spec.name.encode(config.ENCODING), integerkey=db_spec.integerkey
            )

        return True

    def get_db_total_size(self, pretty: bool = True) -> str:
        if not self.split_subdatabases:
            env_i = next(iter(self.env.values()))
            tmp = env_i.info().get("map_size")
        else:
            tmp = sum([env_i.info().get("map_size") for env_i in self.env.values()])
        if not pretty:
            return tmp

        if not tmp:
            return "Unknown"
        return utils.get_file_size(tmp)

    def get_db_size(self, db_name, pretty: bool = True):
        tmp = self.env[db_name].info().get("map_size")
        if not pretty:
            return tmp
        if not tmp:
            return "Unknown"
        return utils.get_file_size(tmp)

    def get_number_items_from(self, db_name: str):
        with self.env[db_name].begin(db=self.dbs[db_name]) as txn:
            return txn.stat()["entries"]

    def close(self):
        self.save_buff()
        if self.split_subdatabases:
            for env_i in self.env.values():
                env_i.close()

    def compress(self) -> None:
        """
        Copy current env to new one (reduce file size)
        :return:
        :rtype:
        """
        if not self.split_subdatabases:
            size_org = os.stat(self.db_file).st_size
            new_dir = self.db_file + ".copy"
            env_i = next(iter(self.env.values()))
            env_i.copy(path=new_dir, compact=True)
            try:
                if os.path.exists(self.db_file):
                    os.remove(self.db_file)
            except Exception as message:
                print(message)
            os.rename(new_dir, self.db_file)
            size_curr = os.stat(self.db_file).st_size
            print(
                f"Compressed: {100 - size_curr / size_org *100:.2f}% - {utils.get_file_size(size_curr)}/{utils.get_file_size(size_org)}"
            )
        else:
            total_org, total_cur = 0, 0
            for db_name in self.db_schema.keys():
                org_dir = self.db_file + f"_{db_name}"
                new_dir = org_dir + ".copy"

                size_org = os.stat(org_dir).st_size
                total_org += size_org
                self.env[db_name].copy(path=new_dir, compact=True)
                try:
                    if os.path.exists(org_dir):
                        os.remove(org_dir)
                except Exception as message:
                    print(message)
                os.rename(new_dir, org_dir)
                size_curr = os.stat(org_dir).st_size

                total_cur += size_curr
                print(
                    f"{db_name} : {100 - size_curr / size_org *100:.2f}% - {utils.get_file_size(size_curr)}/{utils.get_file_size(size_org)}"
                )
            print(
                f"Compressed: {100 - total_cur / total_org *100:.2f}% - {utils.get_file_size(total_cur)}/{utils.get_file_size(total_org)}"
            )

    def get_random_key(self, db_name) -> Any:
        db_schema = dataclasses.asdict(self.db_schema[db_name])
        with self.env[db_name].begin(db=self.dbs[db_name], write=False) as txn:
            random_index = random.randint(0, self.get_number_items_from(db_name))
            cur = txn.cursor()
            cur.first()
            key = utils.deserialize_key(cur.key(), **db_schema)
            for i, k in enumerate(cur.iternext(values=False)):
                if i == random_index:
                    key = utils.deserialize_key(k, **db_schema)
                    break
        return key

    def get_iter_integerkey(
        self, db_name: str, from_i: int = 0, to_i: int = -1, get_values: bool = True
    ) -> Iterator:
        db_schema = dataclasses.asdict(self.db_schema[db_name])
        with self.env[db_name].begin(db=self.dbs[db_name], write=False) as txn:
            if to_i == -1:
                to_i = self.get_number_items_from(db_name)
            cur = txn.cursor()
            cur.set_range(utils.serialize_key(from_i, **db_schema))
            for item in cur.iternext(values=get_values):
                if get_values:
                    key, value = item
                else:
                    key = item
                key = utils.deserialize_key(key, **db_schema)
                if not isinstance(key, int):
                    raise ValueError(
                        f"This function used for integerkey databases. This is {type(key)} key database"
                    )
                if key > to_i:
                    break
                if get_values:
                    value = utils.deserialize_value(value, **db_schema)
                    yield key, value
                else:
                    yield key
            cur.next()

    def get_iter_with_prefix(
        self, db_name: str, prefix: Any, get_values=True
    ) -> Iterator:
        db_schema = dataclasses.asdict(self.db_schema[db_name])

        with self.env[db_name].begin(db=self.dbs[db_name], write=False) as txn:
            cur = txn.cursor()
            prefix = utils.serialize_key(prefix, **db_schema)
            cur.set_range(prefix)

            while cur.key().startswith(prefix) is True:
                try:
                    if cur.key() and not cur.key().startswith(prefix):
                        continue
                    key = utils.deserialize_key(
                        cur.key(),
                        **db_schema,
                    )
                    if get_values:
                        value = utils.deserialize_value(cur.value(), **db_schema)
                        yield key, value
                    else:
                        yield key
                except Exception as message:
                    print(message)
                cur.next()

    def is_available(self, db_name: str, key_obj: str) -> bool:
        db_schema = dataclasses.asdict(self.db_schema[db_name])
        with self.env[db_name].begin(db=self.dbs[db_name]) as txn:
            key_obj = utils.serialize_key(key_obj, **db_schema)
            if key_obj:
                try:
                    value_obj = txn.get(key_obj)
                    if value_obj:
                        return True
                except Exception as message:
                    print(message)
        return False

    def get_value_byte_size(self, db_name: str, key_obj: Any) -> Union[int, None]:
        db_schema = dataclasses.asdict(self.db_schema[db_name])
        with self.env[db_name].begin(db=self.dbs[db_name], buffers=True) as txn:
            key_obj = utils.serialize_key(key_obj, **db_schema)
            if key_obj:
                try:
                    value_obj = txn.get(key_obj)
                    if value_obj:
                        return len(value_obj)
                except Exception as message:
                    print(message)
            return None

    def get_values(self, db_name: str, key_objs: List, get_deserialize: bool = True):
        db_schema = dataclasses.asdict(self.db_schema[db_name])
        with self.env[db_name].begin(db=self.dbs[db_name], buffers=True) as txn:
            if isinstance(key_objs, numpy.ndarray):
                key_objs = key_objs.tolist()
            responds = dict()

            if not (
                isinstance(key_objs, list)
                or isinstance(key_objs, set)
                or isinstance(key_objs, tuple)
            ):
                return responds

            key_objs = [utils.serialize_key(k, **db_schema) for k in key_objs]
            for k, v in txn.cursor(self.dbs[db_name]).getmulti(key_objs):
                if not v:
                    continue
                k = utils.deserialize_key(k, **db_schema)
                if get_deserialize:
                    try:
                        v = utils.deserialize_value(v, **db_schema)
                    except Exception as message:
                        print(message)
                responds[k] = v

        return responds

    def get_value(self, db_name: str, key_obj: Any, get_deserialize: bool = True):
        db_schema = dataclasses.asdict(self.db_schema[db_name])
        with self.env[db_name].begin(db=self.dbs[db_name], buffers=True) as txn:
            key_obj = utils.serialize_key(key_obj, **db_schema)
            responds = None
            if not key_obj:
                return responds
            try:
                value_obj = txn.get(key_obj)
                if not value_obj:
                    return responds
                responds = value_obj
                if get_deserialize:
                    responds = utils.deserialize_value(value_obj, **db_schema)

            except Exception as message:
                print(message)

        return responds

    def head(self, db_name: str, n: int = 5, from_i: int = 0):
        respond = defaultdict()
        for i, (k, v) in enumerate(self.get_db_iter(db_name, from_i=from_i)):
            respond[k] = v
            if i == n - 1:
                break
        return respond

    def get_db_iter(
        self,
        db_name: str,
        get_values: bool = True,
        deserialize_obj: bool = True,
        from_i: int = 0,
        to_i: int = -1,
    ):
        db_schema = dataclasses.asdict(self.db_schema[db_name])
        if to_i == -1:
            to_i = self.get_number_items_from(db_name)

        with self.env[db_name].begin(db=self.dbs[db_name]) as txn:
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
                        key = utils.deserialize_key(key, **db_schema)
                        if get_values:
                            value = utils.deserialize_value(value, **db_schema)
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

    def delete(self, db_name: str, key: Any, with_prefix: bool = False) -> Any:
        db_schema = dataclasses.asdict(self.db_schema[db_name])
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
        with self.env[db_name].begin(
            db=self.dbs[db_name], write=True, buffers=True
        ) as txn:
            for k in key:
                try:
                    status = txn.delete(utils.serialize_key(k, **db_schema))
                    if status:
                        deleted_items += 1
                except Exception as message:
                    print(message)
        return deleted_items

    @staticmethod
    def write(
        env, db, data, sort_key: bool = True, one_sample_write: bool = False, **kargs
    ):
        data = utils.preprocess_data_before_dump(data, sort_key=sort_key, **kargs)
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
            new_limit = curr_limit + config.LMDB_BUFF_LIMIT
            env.set_mapsize(new_limit)
            return FReadDB.write(env, db, data, sort_key=False)
        except lmdb.BadValsizeError:
            print(lmdb.BadValsizeError)
        except lmdb.BadTxnError:
            if one_sample_write:
                return FReadDB.write(
                    env,
                    db,
                    data,
                    sort_key=False,
                    one_sample_write=True,
                )
        except Exception:
            raise Exception
        return added_items

    @staticmethod
    def write_with_buffer(
        env,
        db,
        data,
        sort_key: bool = True,
        show_progress: bool = True,
        step: int = 10000,
        message: str = "DB Write",
        **kwargs,
    ) -> bool:
        data = utils.preprocess_data_before_dump(
            data,
            sort_key=sort_key,
            **kwargs,
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
        return True

    def update_bulk_with_buffer(
        self,
        db_name,
        data,
        update_type=config.DBUpdateType.SET,
        show_progress: bool = True,
        step: int = 10000,
        message="",
        buff_limit=config.LMDB_BUFF_LIMIT,
    ) -> bool:
        db = self.dbs[db_name]
        db_schema = dataclasses.asdict(self.db_schema[db_name])

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
                **db_schema,
            )

            c_buff += len(k) + len(v)
            buff.append((k, v))

            if c_buff >= buff_limit:
                FReadDB.write(self.env[db_name], db, buff)
                buff = []
                c_buff = 0

        if buff:
            FReadDB.write(self.env[db_name], db, buff)
        if show_progress:
            p_bar.set_description(desc=update_desc())
            p_bar.close()
        return True

    def drop_db(self, db_name: str) -> bool:
        with self.env[db_name].begin(write=True) as in_txn:
            in_txn.drop(self.dbs[db_name])
            print(in_txn.stat())
        return True

    def save_buff(self) -> bool:
        for db_name, buff in self.buff.items():
            self.write(
                self.env[db_name],
                self.dbs[db_name],
                buff,
                **dataclasses.asdict(self.db_schema[db_name]),
            )
        return True

    def add(self, db_name, key, value) -> bool:
        db_schema = dataclasses.asdict(self.db_schema[db_name])

        value = utils.serialize_value(value, **db_schema)
        self.buff_size += len(value)
        self.buff[db_name].append([key, value])
        if self.buff_size > self.buff_limit:
            self.save_buff()
            del self.buff
            gc.collect()
            self.buff = defaultdict(list)
            self.buff_size = 0
        return True
