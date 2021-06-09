import abc
import json
import pickle
import zlib

import orjson


class Serializer(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def serialize(cls, x: list) -> bytes:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, y: bytes) -> list:
        raise NotImplementedError


class PickleSerializer(Serializer):
    @classmethod
    def serialize(cls, x):
        return pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def deserialize(cls, y):
        return pickle.loads(y)


class CompressedPickleSerializer(Serializer):
    @classmethod
    def serialize(cls, x):
        return zlib.compress(
            pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL),
            level=3)

    @classmethod
    def deserialize(cls, y):
        return pickle.loads(zlib.decompress(y))


class JsonSerializer(Serializer):
    @classmethod
    def serialize(cls, x):
        return json.dumps(x).encode()

    @classmethod
    def deserialize(cls, y):
        return json.loads(y.decode())


class OrjsonSerializer(Serializer):
    @classmethod
    def serialize(cls, x):
        return orjson.dumps(x)

    @classmethod
    def deserialize(cls, y):
        return orjson.loads(y)


class CompressedOrjsonSerializer(Serializer):
    @classmethod
    def serialize(cls, x):
        return zlib.compress(
            orjson.dumps(x),
            level=3)

    @classmethod
    def deserialize(cls, y):
        return orjson.loads(zlib.decompress(y))
