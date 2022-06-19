import pickle
import struct

import msgpack
import numpy
from lz4 import frame
from pyroaring import BitMap

from freaddb import config


def is_byte_obj(obj):
    if isinstance(obj, bytes) or isinstance(obj, bytearray):
        return True
    return False


def set_default(obj):
    if isinstance(obj, set):
        return sorted(list(obj))
    raise TypeError


def deserialize_key(key, integerkey=False, is_64bit=False):
    if not integerkey:
        if isinstance(key, memoryview):
            key = key.tobytes()
        return key.decode(config.ENCODING)
    if is_64bit:
        return struct.unpack("Q", key)[0]
    else:
        return struct.unpack("I", key)[0]


def deserialize_value(value, bytes_value=config.ToBytesType.OBJ, compress_value=False):
    if bytes_value == config.ToBytesType.INT_NUMPY:
        value = numpy.frombuffer(value, dtype=numpy.uint32)

    elif bytes_value == config.ToBytesType.INT_BITMAP:
        if not isinstance(value, bytes):
            value = bytes(value)
        value = BitMap.deserialize(value)

    elif bytes_value == config.ToBytesType.BYTES:
        if isinstance(value, memoryview):
            value = value.tobytes()

    else:  # mode == "msgpack"
        if compress_value:
            try:
                value = frame.decompress(value)
            except RuntimeError:
                pass
        if bytes_value == config.ToBytesType.PICKLE:
            value = pickle.loads(value)
        else:
            value = msgpack.unpackb(value, strict_map_key=False)
    return value


def deserialize(
    key,
    value,
    integerkey=False,
    is_64bit=False,
    bytes_value=config.ToBytesType.OBJ,
    compress_value=False,
):
    key = deserialize_key(key, integerkey, is_64bit)
    value = deserialize_value(value, bytes_value, compress_value)
    res_obj = (key, value)
    return res_obj


def serialize_key(key, integerkey=False, is_64bit=False):
    if not integerkey:
        if not isinstance(key, str):
            key = str(key)
        return key.encode(config.ENCODING)[: config.LMDB_MAX_KEY]
    if is_64bit:
        return struct.pack("Q", key)
    else:
        return struct.pack("I", key)


def serialize_value(
    value, bytes_value=config.ToBytesType.OBJ, compress_value=False, sort_values=True
):
    if bytes_value == config.ToBytesType.INT_NUMPY:
        if sort_values:
            value = sorted(list(value))
        if not isinstance(value, numpy.ndarray):
            value = numpy.array(value, dtype=numpy.uint32)
        value = value.tobytes()

    elif bytes_value == config.ToBytesType.INT_BITMAP:
        value = BitMap(value).serialize()

    else:  # mode == "msgpack"
        if bytes_value == config.ToBytesType.PICKLE:
            value = pickle.dumps(value)
        else:
            if not isinstance(value, bytes) and not isinstance(value, bytearray):
                value = msgpack.packb(value, default=set_default)
        if compress_value:
            value = frame.compress(value)

    return value


def serialize(
    key,
    value,
    integerkey=False,
    is_64bit=False,
    bytes_value=config.ToBytesType.OBJ,
    compress_value=False,
):
    key = serialize_key(key, integerkey, is_64bit)
    value = serialize_value(value, bytes_value, compress_value)
    res_obj = (key, value)
    return res_obj


def preprocess_data_before_dump(
    data,
    integerkey=False,
    is_64bit=False,
    bytes_value=config.ToBytesType.OBJ,
    compress_value=False,
    sort_key=True,
):
    if isinstance(data, dict):
        data = list(data.items())

    if sort_key and integerkey:
        data.sort(key=lambda x: x[0])

    first_key = data[0][0]
    if not is_byte_obj(first_key):
        data = [
            (serialize_key(k, integerkey=integerkey, is_64bit=is_64bit,), v)
            for k, v in data
            if k is not None
        ]

    first_value = data[0][0]
    if not is_byte_obj(first_value):
        data = [
            serialize_value(v, bytes_value=bytes_value, compress_value=compress_value,)
            for k, v in data
            if k is not None
        ]

    if sort_key and not integerkey:
        data.sort(key=lambda x: x[0])

    return data
