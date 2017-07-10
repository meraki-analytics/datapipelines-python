from typing import Generic, TypeVar, Any

TYPE_WILDCARD = Any


class UnsupportedError(ValueError):
    pass


class NotFoundError(ValueError):
    pass


class PipelineContext(dict):
    class Keys(object):
        PIPELINE = "pipeline"
        EXPIRATION = "expires"


T = TypeVar("T")
Q = TypeVar("Q")


class TypePair(Generic[T, Q]):
    pass
