import random
from typing import Type, TypeVar, Mapping, Any, Iterable, Generator

import pytest
from datapipelines import DataSource, PipelineContext, NotFoundError

#########################################
# Create simple DataSources for testing #
#########################################

T = TypeVar("T")

VALUE_KEY = "value"
COUNT_KEY = "count"

VALUES_COUNT = 100
VALUES_MAX = 10000


class TestWildcardDataSource(DataSource):
    def get(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> T:
        value = query.get(VALUE_KEY)

        try:
            # noinspection PyCallingNonCallable
            return type(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"{type}\"".format(type=type))

    def get_many(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> Generator[T, None, None]:
        value = query.get(VALUE_KEY)
        count = query.get(COUNT_KEY)

        try:
            # noinspection PyCallingNonCallable
            value = type(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"{type}\"".format(type=type))

        return (value for _ in range(count))


class TestDataSource(DataSource):
    @DataSource.dispatch
    def get(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> T:
        pass

    @DataSource.dispatch
    def get_many(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        pass

    @get.register(int)
    def _(self, query: Mapping[str, Any], context: PipelineContext = None) -> int:
        value = query.get(VALUE_KEY)

        try:
            return int(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"int\"")

    @get_many.register(int)
    def _(self, query: Mapping[str, Any], context: PipelineContext = None) -> Generator[int, None, None]:
        value = query.get(VALUE_KEY)
        count = query.get(COUNT_KEY)

        try:
            value = int(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"int\"")

        return (value for _ in range(count))

    @get.register(float)
    def _(self, query: Mapping[str, Any], context: PipelineContext = None) -> float:
        value = query.get(VALUE_KEY)

        try:
            return float(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"float\"")

    @get_many.register(float)
    def _(self, query: Mapping[str, Any], context: PipelineContext = None) -> Generator[float, None, None]:
        value = query.get(VALUE_KEY)
        count = query.get(COUNT_KEY)

        try:
            value = float(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"float\"")

        return (value for _ in range(count))


########################
# Unsupported Function #
########################

def test_unsupported():
    from datapipelines import UnsupportedError

    unsupported = DataSource.unsupported(int)
    assert type(unsupported) is UnsupportedError

    unsupported = DataSource.unsupported(float)
    assert type(unsupported) is UnsupportedError

    unsupported = DataSource.unsupported(str)
    assert type(unsupported) is UnsupportedError


#####################
# Provides Function #
#####################

def test_provides():
    source = TestDataSource()

    assert source.provides == {int, float}


def test_wildcard_provides():
    from datapipelines import TYPE_WILDCARD
    source = TestWildcardDataSource()

    assert source.provides is TYPE_WILDCARD


################
# Get Function #
################

def test_get():
    source = TestDataSource()

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(int, query)

        assert type(result) is int
        assert result == value

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(float, query)

        assert type(result) is float
        assert result == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(int, query)

        assert type(result) is int
        assert result == int(value)

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(float, query)

        assert type(result) is float
        assert result == value


def test_get_unsupported():
    from datapipelines import UnsupportedError
    source = TestDataSource()

    query = {VALUE_KEY: "test"}

    with pytest.raises(UnsupportedError):
        source.get(str, query)


def test_wildcard_get():
    source = TestWildcardDataSource()

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(int, query)

        assert type(result) is int
        assert result == value

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(float, query)

        assert type(result) is float
        assert result == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(int, query)

        assert type(result) is int
        assert result == int(value)

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(float, query)

        assert type(result) is float
        assert result == value


#####################
# Get Many Function #
#####################

def test_get_many():
    source = TestDataSource()

    # Seriously where is this in the std lib...
    generator_class = (None for _ in range(0)).__class__

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(int, query)

        assert type(result) is generator_class
        for res in result:
            assert type(res) is int
            assert res == value

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(float, query)

        assert type(result) is generator_class
        for res in result:
            assert type(res) is float
            assert res == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(int, query)

        assert type(result) is generator_class
        for res in result:
            assert type(res) is int
            assert res == int(value)

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(float, query)

        assert type(result) is generator_class
        for res in result:
            assert type(res) is float
            assert res == value


def test_get_many_unsupported():
    from datapipelines import UnsupportedError
    source = TestDataSource()

    query = {VALUE_KEY: "test", COUNT_KEY: VALUES_COUNT}

    with pytest.raises(UnsupportedError):
        source.get_many(str, query)


def test_wildcard_get_many():
    source = TestWildcardDataSource()

    # Seriously where is this in the std lib...
    generator_class = (None for _ in range(0)).__class__

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(int, query)

        assert type(result) is generator_class
        for res in result:
            assert type(res) is int
            assert res == value

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(float, query)

        assert type(result) is generator_class
        for res in result:
            assert type(res) is float
            assert res == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(int, query)

        assert type(result) is generator_class
        for res in result:
            assert type(res) is int
            assert res == int(value)

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(float, query)

        assert type(result) is generator_class
        for res in result:
            assert type(res) is float
            assert res == value
