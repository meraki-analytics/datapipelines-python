import random
from typing import Type, TypeVar, Iterable

import pytest
from datapipelines import DataSink, PipelineContext

#######################################
# Create simple DataSinks for testing #
#######################################

T = TypeVar("T")

VALUES_COUNT = 100
VALUES_MAX = 100000000


class SimpleWildcardDataSink(DataSink):
    def __init__(self) -> None:
        self.items = {}

    def put(self, type: Type[T], item: T, context: PipelineContext = None) -> None:
        try:
            for_type = self.items[type]
        except KeyError:
            for_type = set()
            self.items[type] = for_type

        for_type.add(item)

    def put_many(self, type: Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        try:
            for_type = self.items[type]
        except KeyError:
            for_type = set()
            self.items[type] = for_type

        for item in items:
            for_type.add(item)


class SimpleDataSink(DataSink):
    def __init__(self) -> None:
        self.items = {
            int: set(),
            float: set()
        }

    @DataSink.dispatch
    def put(self, type: Type[T], item: T, context: PipelineContext = None) -> None:
        pass

    @DataSink.dispatch
    def put_many(self, type: Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        pass

    @put.register(int)
    def put_int(self, item: int, context: PipelineContext = None) -> None:
        self.items[int].add(item)

    @put_many.register(int)
    def put_many_int(self, items: Iterable[int], context: PipelineContext = None) -> None:
        for item in items:
            self.items[int].add(item)

    @put.register(float)
    def put_float(self, item: float, context: PipelineContext = None) -> None:
        self.items[float].add(item)

    @put_many.register(float)
    def put_many_float(self, items: Iterable[float], context: PipelineContext = None) -> None:
        for item in items:
            self.items[float].add(item)


########################
# Unsupported Function #
########################

def test_unsupported():
    from datapipelines import UnsupportedError

    unsupported = DataSink.unsupported(int)
    assert type(unsupported) is UnsupportedError

    unsupported = DataSink.unsupported(float)
    assert type(unsupported) is UnsupportedError

    unsupported = DataSink.unsupported(str)
    assert type(unsupported) is UnsupportedError


####################
# Accepts Function #
####################

def test_accepts():
    sink = SimpleDataSink()

    assert sink.accepts == {int, float}


def test_wildcard_accepts():
    from datapipelines import TYPE_WILDCARD
    sink = SimpleWildcardDataSink()

    assert sink.accepts is TYPE_WILDCARD


################
# Put Function #
################

def test_put():
    sink = SimpleDataSink()

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        result = sink.put(int, value)

        assert result is None
        assert value in sink.items[int]

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        result = sink.put(float, value)

        assert result is None
        assert value in sink.items[float]


def test_put_unsupported():
    from datapipelines import UnsupportedError
    sink = SimpleDataSink()

    with pytest.raises(UnsupportedError):
        sink.put(str, "test")


def test_wildcard_put():
    sink = SimpleWildcardDataSink()

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        result = sink.put(int, value)

        assert result is None
        assert value in sink.items[int]

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        result = sink.put(float, value)

        assert result is None
        assert value in sink.items[float]


#####################
# Put Many Function #
#####################

def test_put_many():
    sink = SimpleDataSink()

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        items = (value for _ in range(VALUES_COUNT))
        result = sink.put_many(int, items)

        assert result is None
        assert value in sink.items[int]

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        items = (value for _ in range(VALUES_COUNT))
        result = sink.put_many(float, items)

        assert result is None
        assert value in sink.items[float]


def test_put_many_unsupported():
    from datapipelines import UnsupportedError
    sink = SimpleDataSink()

    with pytest.raises(UnsupportedError):
        sink.put_many(str, ("test" for _ in range(VALUES_COUNT)))


def test_wildcard_put_many():
    sink = SimpleWildcardDataSink()

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        items = (value for _ in range(VALUES_COUNT))
        result = sink.put_many(int, items)

        assert result is None
        assert value in sink.items[int]

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        items = (value for _ in range(VALUES_COUNT))
        result = sink.put_many(float, items)

        assert result is None
        assert value in sink.items[float]
