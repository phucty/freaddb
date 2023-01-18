import math
import os
import pickle
import struct
from datetime import datetime
from typing import Any, ByteString, Callable, List, Tuple, Union

import msgpack
import numpy
import psutil
from lz4 import frame
from pyroaring import BitMap

from freaddb import config
from freaddb.config import ToBytes


def is_byte_obj(obj: Any) -> bool:
    if isinstance(obj, bytes) or isinstance(obj, bytearray):
        return True
    return False


def deserialize_key(
    key: Any,
    integerkey=False,
    is_64bit=False,
    combinekey: bool = False,
    combinelen: int = 2,
    **kawgs,
) -> Union[int, str]:

    # String key
    if not integerkey:
        if isinstance(key, memoryview):
            key = key.tobytes()
        return key.decode(config.ENCODING)

    # Integer key
    format_template = "Q" if is_64bit else "I"
    # Tuple integer key
    if combinekey:
        format_template = format_template * combinelen
        return struct.unpack(format_template, key)

    # Single integer key
    return struct.unpack(format_template, key)[0]


def deserialize_value(
    value: ByteString,
    bytes_value: ToBytes = ToBytes.OBJ,
    compress_value: bool = False,
    **kawgs,
) -> Any:
    if bytes_value == config.ToBytes.INT_NUMPY:
        value = numpy.frombuffer(value, dtype=numpy.uint32)

    elif bytes_value == config.ToBytes.INT_BITMAP:
        if not isinstance(value, bytes):
            value = bytes(value)
        value = BitMap.deserialize(value)

    elif bytes_value == config.ToBytes.BYTES:
        if isinstance(value, memoryview):
            value = value.tobytes()

    else:  # mode == "msgpack"
        if compress_value:
            try:
                value = frame.decompress(value)
            except RuntimeError:
                pass
        if bytes_value == config.ToBytes.PICKLE:
            value = pickle.loads(value)
        else:
            value = msgpack.unpackb(value, strict_map_key=False)
    return value


def deserialize(
    key: ByteString,
    value: ByteString,
    integerkey: bool = False,
    combinekey: bool = False,
    combinelen: int = 2,
    is_64bit: bool = False,
    bytes_value: ToBytes = config.ToBytes.OBJ,
    compress_value: bool = False,
    **kawgs,
) -> Tuple[Any, Any]:
    key = deserialize_key(
        key=key,
        integerkey=integerkey,
        is_64bit=is_64bit,
        combinekey=combinekey,
        combinelen=combinelen,
    )
    value = deserialize_value(
        value=value, bytes_value=bytes_value, compress_value=compress_value
    )
    res_obj = (key, value)
    return res_obj


def serialize_key(
    key: Any,
    integerkey: bool = False,
    combinekey: bool = False,
    is_64bit: bool = False,
    combinelen: int = 2,
    **kawgs,
) -> ByteString:
    if not integerkey:
        if not isinstance(key, str):
            key = str(key)
        return key.encode(config.ENCODING)[: config.LMDB_MAX_KEY]

    # Integer key
    format_template = "Q" if is_64bit else "I"
    # Tuple integer key
    if combinekey:
        format_template = format_template * combinelen
        return struct.pack(format_template, *key)

    if (
        not isinstance(key, int)
        and not isinstance(key, list)
        and not isinstance(key, tuple)
    ):
        raise TypeError

    return struct.pack(format_template, key)


def serialize_value(
    value: Any,
    bytes_value: ToBytes = ToBytes.OBJ,
    compress_value: bool = False,
    sort_values: bool = True,
    **kawgs,
) -> ByteString:
    def set_default(obj):
        if isinstance(obj, set):
            return sorted(list(obj))
        raise TypeError

    if bytes_value == config.ToBytes.INT_NUMPY:
        if sort_values:
            value = sorted(list(value))
        if not isinstance(value, numpy.ndarray):
            value = numpy.array(value, dtype=numpy.uint32)
        value = value.tobytes()

    elif bytes_value == config.ToBytes.INT_BITMAP:
        value = BitMap(value).serialize()

    else:  # mode == "msgpack"
        if bytes_value == config.ToBytes.PICKLE:
            value = pickle.dumps(value)
        else:
            if not isinstance(value, bytes) and not isinstance(value, bytearray):
                value = msgpack.packb(value, default=set_default)
        if compress_value:
            value = frame.compress(value)

    return value


def serialize(
    key: Any,
    value: Any,
    integerkey: bool = False,
    combinekey: bool = False,
    combinelen: int = 2,
    is_64bit: bool = False,
    bytes_value: ToBytes = ToBytes.OBJ,
    compress_value: bool = False,
    **kawgs,
) -> Tuple[ByteString, ByteString]:
    key = serialize_key(
        key=key,
        integerkey=integerkey,
        combinekey=combinekey,
        combinelen=combinelen,
        is_64bit=is_64bit,
    )
    value = serialize_value(
        value=value, bytes_value=bytes_value, compress_value=compress_value
    )
    res_obj = (key, value)
    return res_obj


def preprocess_data_before_dump(
    data: List[Any],
    integerkey: bool = False,
    combinekey: bool = False,
    combinelen: int = 2,
    is_64bit: bool = False,
    bytes_value: ToBytes = ToBytes.OBJ,
    compress_value: bool = False,
    sort_key: bool = True,
    **kwargs,
) -> List[Any]:
    if isinstance(data, dict):
        data = list(data.items())

    if sort_key and integerkey:
        data.sort(key=lambda x: x[0])

    first_key, first_value = data[0]
    to_bytes_key = not is_byte_obj(first_key)
    to_bytes_value = not is_byte_obj(first_value)

    for i in range(len(data)):
        k, v = data[i]
        if k is None:
            continue
        if to_bytes_key:
            data[i][0] = serialize_key(
                key=k,
                integerkey=integerkey,
                combinekey=combinekey,
                combinelen=combinelen,
                is_64bit=is_64bit,
            )
        if to_bytes_value:
            data[i][1] = serialize_value(
                value=v,
                bytes_value=bytes_value,
                compress_value=compress_value,
            )

    if sort_key and not integerkey:
        data.sort(key=lambda x: x[0])

    if not isinstance(data[0], tuple):
        data = [(k, v) for k, v in data]
    return data


def get_file_size(num: int, suffix="B"):
    num = abs(num)
    if num == 0:
        return "0B"
    try:
        magnitude = int(math.floor(math.log(num, 1024)))
        val = num / math.pow(1024, magnitude)
        if magnitude > 7:
            return "{:3.1f}{}{}".format(val, "Yi", suffix)
        return "{:3.1f}{}{}".format(
            val, ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"][magnitude], suffix
        )
    except ValueError:
        print(num)
        return "0B"


def profile(func: Callable):
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info()
        start = datetime.now()

        result = func(*args, **kwargs)

        end = datetime.now() - start
        mem_after = process.memory_info()
        rss = get_file_size(mem_after.rss - mem_before.rss)
        vms = get_file_size(mem_after.vms - mem_before.vms)
        print(f"{func.__name__}\tTime: {end}\tRSS: {rss}\tVMS: {vms}")
        return result

    return wrapper
