import random
from typing import Type, TypeVar, Mapping, Any, Iterable, Generator

import pytest

from datapipelines import DataSource, CompositeDataSource, PipelineContext, NotFoundError

#########################################
# Create simple DataSources for testing #
#########################################

T = TypeVar("T")

VALUE_KEY = "value"
COUNT_KEY = "count"

VALUES_COUNT = 100
VALUES_MAX = 100000000

# Seriously where is this in the std lib...
GENERATOR_CLASS = (None for _ in range(0)).__class__


class SimpleWildcardDataSource(DataSource):
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


class IntFloatDataSource(DataSource):
    @DataSource.dispatch
    def get(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> T:
        pass

    @DataSource.dispatch
    def get_many(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        pass

    @get.register(int)
    def get_int(self, query: Mapping[str, Any], context: PipelineContext = None) -> int:
        value = query.get(VALUE_KEY)

        try:
            return int(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"int\"")

    @get_many.register(int)
    def get_many_int(self, query: Mapping[str, Any], context: PipelineContext = None) -> Generator[int, None, None]:
        value = query.get(VALUE_KEY)
        count = query.get(COUNT_KEY)

        try:
            value = int(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"int\"")

        return (value for _ in range(count))

    @get.register(float)
    def get_float(self, query: Mapping[str, Any], context: PipelineContext = None) -> float:
        value = query.get(VALUE_KEY)

        try:
            return float(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"float\"")

    @get_many.register(float)
    def get_many_float(self, query: Mapping[str, Any], context: PipelineContext = None) -> Generator[float, None, None]:
        value = query.get(VALUE_KEY)
        count = query.get(COUNT_KEY)

        try:
            value = float(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"float\"")

        return (value for _ in range(count))


class StringDataSource(DataSource):
    @DataSource.dispatch
    def get(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> T:
        pass

    @DataSource.dispatch
    def get_many(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        pass

    @get.register(str)
    def get_str(self, query: Mapping[str, Any], context: PipelineContext = None) -> str:
        value = query.get(VALUE_KEY)

        try:
            return str(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"str\"")

    @get_many.register(str)
    def get_many_str(self, query: Mapping[str, Any], context: PipelineContext = None) -> Generator[str, None, None]:
        value = query.get(VALUE_KEY)
        count = query.get(COUNT_KEY)

        try:
            value = str(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"str\"")

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
    source = IntFloatDataSource()

    assert source.provides == {int, float}


def test_wildcard_provides():
    from datapipelines import TYPE_WILDCARD
    source = SimpleWildcardDataSource()

    assert source.provides is TYPE_WILDCARD


################
# Get Function #
################

def test_get():
    source = IntFloatDataSource()

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(int, query)

        assert type(result) is int
        assert result == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(float, query)

        assert type(result) is float
        assert result == value


def test_get_unsupported():
    from datapipelines import UnsupportedError
    source = IntFloatDataSource()

    query = {VALUE_KEY: "test"}

    with pytest.raises(UnsupportedError):
        source.get(str, query)


def test_wildcard_get():
    source = SimpleWildcardDataSource()

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(int, query)

        assert type(result) is int
        assert result == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(float, query)

        assert type(result) is float
        assert result == value


#####################
# Get Many Function #
#####################

def test_get_many():
    source = IntFloatDataSource()

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(int, query)

        assert type(result) is GENERATOR_CLASS
        for res in result:
            assert type(res) is int
            assert res == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(float, query)

        assert type(result) is GENERATOR_CLASS
        for res in result:
            assert type(res) is float
            assert res == value


def test_get_many_unsupported():
    from datapipelines import UnsupportedError
    source = IntFloatDataSource()

    query = {VALUE_KEY: "test", COUNT_KEY: VALUES_COUNT}

    with pytest.raises(UnsupportedError):
        source.get_many(str, query)


def test_wildcard_get_many():
    source = SimpleWildcardDataSource()

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(int, query)

        assert type(result) is GENERATOR_CLASS
        for res in result:
            assert type(res) is int
            assert res == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(float, query)

        assert type(result) is GENERATOR_CLASS
        for res in result:
            assert type(res) is float
            assert res == value


#######################
# CompositeDataSource #
#######################

def test_composite_provides():
    source = CompositeDataSource({IntFloatDataSource(), StringDataSource()})
    assert source.provides == {int, float, str}


def test_composite_get():
    source = CompositeDataSource({IntFloatDataSource(), StringDataSource()})

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(int, query)

        assert type(result) is int
        assert result == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(float, query)

        assert type(result) is float
        assert result == value

    values = [str(random.uniform(-VALUES_MAX, VALUES_MAX)) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = source.get(str, query)

        assert type(result) is str
        assert result == value


def test_composite_get_unsupported():
    from datapipelines import UnsupportedError
    source = CompositeDataSource({IntFloatDataSource(), StringDataSource()})

    query = {VALUE_KEY: bytes()}

    with pytest.raises(UnsupportedError):
        source.get(bytes, query)


def test_composite_get_many():
    source = CompositeDataSource({IntFloatDataSource(), StringDataSource()})

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(int, query)

        assert type(result) is GENERATOR_CLASS
        for res in result:
            assert type(res) is int
            assert res == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(float, query)

        assert type(result) is GENERATOR_CLASS
        for res in result:
            assert type(res) is float
            assert res == value

    values = [str(random.uniform(-VALUES_MAX, VALUES_MAX)) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = source.get_many(str, query)

        assert type(result) is GENERATOR_CLASS
        for res in result:
            assert type(res) is str
            assert res == value


def test_composite_get_many_unsupported():
    from datapipelines import UnsupportedError
    source = CompositeDataSource({IntFloatDataSource(), StringDataSource()})

    query = {VALUE_KEY: bytes(), COUNT_KEY: VALUES_COUNT}

    with pytest.raises(UnsupportedError):
        source.get_many(bytes, query)
