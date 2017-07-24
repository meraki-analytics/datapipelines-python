import random
from typing import Type, TypeVar, Mapping, Any, Iterable, Generator

import pytest
from networkx import DiGraph

from datapipelines import DataPipeline, DataSource, DataSink, DataTransformer, PipelineContext, NotFoundError, NoConversionError
from datapipelines.pipelines import _build_type_graph, _pairwise, _identity, _transform, _SinkHandler, _SourceHandler

T = TypeVar("T")
F = TypeVar("F")

VALUE_KEY = "value"
COUNT_KEY = "count"

VALUES_COUNT = 100
VALUES_MAX = 100000000

# Seriously where is this in the std lib...
GENERATOR_CLASS = (None for _ in range(0)).__class__


###############################################
# Create simple pipeline elements for testing #
###############################################

class IntSource(DataSource):
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


class FloatStore(DataSource, DataSink):
    def __init__(self) -> None:
        self.items = set()

    @DataSink.dispatch
    def put(self, type: Type[T], item: T, context: PipelineContext = None) -> None:
        pass

    @DataSink.dispatch
    def put_many(self, type: Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        pass

    @put.register(float)
    def put_float(self, item: float, context: PipelineContext = None) -> None:
        self.items.add(item)

    @put_many.register(float)
    def put_many_float(self, items: Iterable[float], context: PipelineContext = None) -> None:
        for item in items:
            self.items.add(item)

    @DataSource.dispatch
    def get(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> T:
        pass

    @DataSink.dispatch
    def get_many(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        pass

    @get.register(float)
    def get_float(self, query: Mapping[str, Any], context: PipelineContext = None) -> float:
        value = query.get(VALUE_KEY)

        try:
            value = float(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"float\"")

        if value not in self.items:
            raise NotFoundError("Query value wasn't in store!")

        return value

    @get_many.register(float)
    def get_many_float(self, query: Mapping[str, Any], context: PipelineContext = None) -> Generator[float, None, None]:
        value = query.get(VALUE_KEY)
        count = query.get(COUNT_KEY)

        try:
            value = float(value)
        except ValueError:
            raise NotFoundError("Couldn't cast the query value to \"float\"")

        if value not in self.items:
            raise NotFoundError("Query value wasn't in store!")

        return (value for _ in range(count))


class IntFloatTransformer(DataTransformer):
    @DataTransformer.dispatch
    def transform(self, target_type: Type[T], value: F, context: PipelineContext = None) -> T:
        pass

    @transform.register(int, float)
    def int_to_float(self, value: int, context: PipelineContext = None) -> float:
        return float(value)


class FloatIntTransformer(DataTransformer):
    @DataTransformer.dispatch
    def transform(self, target_type: Type[T], value: F, context: PipelineContext = None) -> T:
        pass

    @transform.register(float, int)
    def float_to_int(self, value: float, context: PipelineContext = None) -> int:
        return int(value)

    @property
    def cost(self) -> int:
        return 3


class StringTransformer(DataTransformer):
    @DataTransformer.dispatch
    def transform(self, target_type: Type[T], value: F, context: PipelineContext = None) -> T:
        pass

    @transform.register(int, str)
    def int_to_str(self, value: int, context: PipelineContext = None) -> str:
        return str(value)

    @transform.register(float, str)
    def float_to_str(self, value: float, context: PipelineContext = None) -> str:
        return str(value)

    @transform.register(str, int)
    def str_to_int(self, value: str, context: PipelineContext = None) -> int:
        return int(float(value))

    @transform.register(str, float)
    def str_to_float(self, value: str, context: PipelineContext = None) -> float:
        return float(value)


#########################
# Type graph generation #
#########################

def test_build_type_graph():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()
    string = StringTransformer()

    sources = {int_source, float_store}
    sinks = {float_store}
    transformers = {int_float, float_int, string}

    expected = DiGraph()
    expected.add_node(str)
    expected.add_node(int, sources={int_source})
    expected.add_node(float, sources={float_store}, sinks={float_store})
    expected.add_edge(int, float, cost=1, transformer=int_float)
    expected.add_edge(float, int, cost=3, transformer=float_int)
    expected.add_edge(str, int, cost=1, transformer=string)
    expected.add_edge(str, float, cost=1, transformer=string)
    expected.add_edge(int, str, cost=1, transformer=string)
    expected.add_edge(float, str, cost=1, transformer=string)

    # noinspection PyTypeChecker
    actual = _build_type_graph(sources, sinks, transformers)

    for node in expected.nodes():
        assert expected.node[node] == actual.node[node]

    for source, target in expected.edges():
        assert expected.edge[source][target] == actual.edge[source][target]

    for node in actual.nodes():
        assert expected.node[node] == actual.node[node]

    for source, target in actual.edges():
        assert expected.edge[source][target] == actual.edge[source][target]


#####################
# Utility functions #
#####################

def test_pairwise():
    values = (x for x in range(VALUES_COUNT))
    values = _pairwise(values)

    for i in range(VALUES_COUNT - 1):
        assert next(values) == (i, i + 1)


def test_identity():
    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        assert _identity(value) == value

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        assert _identity(value) == value


def test_transform():
    string = StringTransformer()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()

    chain = [(string, str), (string, float), (float_int, int), (string, str), (string, float), (float_int, int), (int_float, float)]
    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]
    for value in values:
        result = _transform(chain, value)

        assert type(result) is float
        assert result == value


###############
# SinkHandler #
###############

def test_sink_handler_put():
    sink = FloatStore()

    def convert(data: int, context: PipelineContext = None) -> float:
        return float(data)

    handler = _SinkHandler(sink, float, convert)

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        result = handler.put(value)

        assert result is None
        assert value in sink.items


def test_sink_handler_put_many():
    sink = FloatStore()

    def convert(data: int, context: PipelineContext = None) -> float:
        return float(data)

    handler = _SinkHandler(sink, float, convert)

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        items = (value for _ in range(VALUES_COUNT))
        result = handler.put_many(items)

        assert result is None
        assert value in sink.items


#################
# SourceHandler #
#################

def test_source_handler_get():
    source = IntSource()
    before_sink = FloatStore()
    after_sink = FloatStore()

    def convert(data: int, context: PipelineContext = None) -> float:
        return float(data)

    sinks = {
        _SinkHandler(before_sink, float, convert): False,
        _SinkHandler(after_sink, float, _identity): True
    }

    handler = _SourceHandler(source, int, convert, sinks)

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}
        result = handler.get(query)

        assert type(result) is float
        floored_value = float(int(value))  # float to int conversion will floor the value
        assert result == floored_value

        assert floored_value in before_sink.items
        assert floored_value in after_sink.items


def test_source_handler_get_many_non_streaming():
    source = IntSource()
    before_sink = FloatStore()
    after_sink = FloatStore()

    def convert(data: int, context: PipelineContext = None) -> float:
        return float(data)

    sinks = {
        _SinkHandler(before_sink, float, convert): False,
        _SinkHandler(after_sink, float, _identity): True
    }

    handler = _SourceHandler(source, int, convert, sinks)

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = handler.get_many(query, streaming=False)

        assert type(result) is list
        floored_value = float(int(value))  # float to int conversion will floor the value
        for res in result:
            assert res == floored_value

            assert floored_value in before_sink.items
            assert floored_value in after_sink.items


def test_source_handler_get_many_streaming():
    source = IntSource()
    before_sink = FloatStore()
    after_sink = FloatStore()

    def convert(data: int, context: PipelineContext = None) -> float:
        return float(data)

    sinks = {
        _SinkHandler(before_sink, float, convert): False,
        _SinkHandler(after_sink, float, _identity): True
    }

    handler = _SourceHandler(source, int, convert, sinks)

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}
        result = handler.get_many(query, streaming=True)

        assert type(result) is GENERATOR_CLASS
        floored_value = float(int(value))  # float to int conversion will floor the value
        for res in result:
            assert res == floored_value

            assert floored_value in before_sink.items
            assert floored_value in after_sink.items


################
# DataPipeline #
################

def test_pipeline_transform():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()
    string = StringTransformer()

    elements = [float_store, int_source]
    transformers = {int_float, float_int, string}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    # Identity of type in graph
    transform, cost = pipeline._transform(int, int)
    assert cost == 0
    assert transform is _identity

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        result = transform(data=value)

        assert type(result) is int
        assert result == value

    # Identity of type not in graph
    transform, cost = pipeline._transform(bool, bool)
    assert cost == 0
    assert transform is _identity

    values = [random.choice([True, False]) for _ in range(VALUES_COUNT)]

    for value in values:
        result = transform(data=value)

        assert type(result) is bool
        assert result == value

    # Simple transform
    transform, cost = pipeline._transform(int, float)
    assert cost == 1

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        result = transform(data=value)

        assert type(result) is float
        assert result == value

    # Avoid expensive transformer
    transform, cost = pipeline._transform(float, int)
    assert cost == 2  # It will go through the StringTransformer twice, which is cheaper than the FloatIntTransformer

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        result = transform(data=value)

        assert type(result) is int
        assert result == int(value)


def test_pipeline_transform_impossible():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()

    elements = [float_store, int_source]
    transformers = {int_float}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    with pytest.raises(NoConversionError):
        pipeline._transform(float, int)

    with pytest.raises(NoConversionError):
        pipeline._transform(str, int)

    with pytest.raises(NoConversionError):
        pipeline._transform(int, str)


def test_get_handlers():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()
    string = StringTransformer()

    elements = [float_store, int_source]
    transformers = {int_float, float_int, string}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    handlers = pipeline._get_handlers(str)
    assert type(handlers) is list
    assert len(handlers) is 2

    assert handlers[0]._source is float_store
    assert handlers[0]._source_type is float

    assert handlers[1]._source is int_source
    assert handlers[1]._source_type is int

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]
    for value in values:
        assert type(handlers[0]._transform(data=value)) is str

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]
    for value in values:
        assert type(handlers[1]._transform(data=value)) is str

    assert len(handlers[0]._before_transform) == 0
    assert len(handlers[0]._after_transform) == 0

    assert len(handlers[1]._before_transform) == 1
    assert len(handlers[1]._after_transform) == 1

    sink_handler = list(handlers[1]._before_transform)[0]

    assert sink_handler._sink is float_store
    assert sink_handler._store_type is float

    for value in values:
        assert type(sink_handler._transform(data=value)) is float


def test_get_handlers_impossible():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()

    elements = [float_store, int_source]
    transformers = {int_float}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    with pytest.raises(NoConversionError):
        pipeline._get_handlers(str)


def test_put_handlers():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()
    string = StringTransformer()

    elements = [float_store, int_source]
    transformers = {int_float, float_int, string}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    handlers = pipeline._put_handlers(str)
    assert type(handlers) is set
    assert len(handlers) is 1

    handler = list(handlers)[0]

    assert handler._sink is float_store
    assert handler._store_type is float

    values = [str(random.uniform(-VALUES_MAX, VALUES_MAX)) for _ in range(VALUES_COUNT)]

    for value in values:
        assert type(handler._transform(data=value)) is float


def test_put_handlers_impossible():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()

    elements = [float_store, int_source]
    transformers = {int_float}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    with pytest.raises(NoConversionError):
        pipeline._put_handlers(str)


def test_new_context():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()
    string = StringTransformer()

    elements = [float_store, int_source]
    transformers = {int_float, float_int, string}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    context = pipeline._new_context()
    assert type(context) is PipelineContext
    assert context[PipelineContext.Keys.PIPELINE] is pipeline


def test_get():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()
    string = StringTransformer()

    elements = [float_store, int_source]
    transformers = {int_float, float_int, string}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    values = [str(random.randint(-VALUES_MAX, VALUES_MAX)) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value}

        result = pipeline.get(int, query)
        assert type(result) is int
        assert result == int(value)

        assert float(value) in float_store.items

    float_store.items.clear()

    for value in values:
        query = {VALUE_KEY: value}

        result = pipeline.get(float, query)
        assert type(result) is float
        assert result == float(value)

        assert float(value) in float_store.items

    float_store.items.clear()

    for value in values:
        query = {VALUE_KEY: value}

        result = pipeline.get(str, query)
        assert type(result) is str
        assert result == value

        assert float(value) in float_store.items


def test_get_many_non_streaming():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()
    string = StringTransformer()

    elements = [float_store, int_source]
    transformers = {int_float, float_int, string}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    values = [str(random.randint(-VALUES_MAX, VALUES_MAX)) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}

        result = pipeline.get_many(int, query, streaming=False)
        assert type(result) is list

        for res in result:
            assert type(res) is int
            assert res == int(value)
            assert float(value) in float_store.items

    float_store.items.clear()

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}

        result = pipeline.get_many(float, query, streaming=False)
        assert type(result) is list

        for res in result:
            assert type(res) is float
            assert res == float(value)
            assert float(value) in float_store.items

    float_store.items.clear()

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}

        result = pipeline.get_many(str, query, streaming=False)
        assert type(result) is list

        for res in result:
            assert type(res) is str
            assert res == value
            assert float(value) in float_store.items


def test_get_many_streaming():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()
    string = StringTransformer()

    elements = [float_store, int_source]
    transformers = {int_float, float_int, string}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    values = [str(random.randint(-VALUES_MAX, VALUES_MAX)) for _ in range(VALUES_COUNT)]

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}

        result = pipeline.get_many(int, query, streaming=True)
        assert type(result) is GENERATOR_CLASS

        for res in result:
            assert type(res) is int
            assert res == int(value)
            assert float(value) in float_store.items

    float_store.items.clear()

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}

        result = pipeline.get_many(float, query, streaming=True)
        assert type(result) is GENERATOR_CLASS

        for res in result:
            assert type(res) is float
            assert res == float(value)
            assert float(value) in float_store.items

    float_store.items.clear()

    for value in values:
        query = {VALUE_KEY: value, COUNT_KEY: VALUES_COUNT}

        result = pipeline.get_many(str, query, streaming=True)
        assert type(result) is GENERATOR_CLASS

        for res in result:
            assert type(res) is str
            assert res == value
            assert float(value) in float_store.items


def test_put():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()
    string = StringTransformer()

    elements = [float_store, int_source]
    transformers = {int_float, float_int, string}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        result = pipeline.put(int, value)

        assert result is None
        assert float(value) in float_store.items

    float_store.items.clear()

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        result = pipeline.put(float, value)

        assert result is None
        assert value in float_store.items

    float_store.items.clear()

    values = [str(random.uniform(-VALUES_MAX, VALUES_MAX)) for _ in range(VALUES_COUNT)]

    for value in values:
        result = pipeline.put(str, value)

        assert result is None
        assert float(value) in float_store.items


def test_put_many():
    int_source = IntSource()
    float_store = FloatStore()
    int_float = IntFloatTransformer()
    float_int = FloatIntTransformer()
    string = StringTransformer()

    elements = [float_store, int_source]
    transformers = {int_float, float_int, string}

    # noinspection PyTypeChecker
    pipeline = DataPipeline(elements, transformers)

    values = [random.randint(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        items = (value for _ in range(VALUES_COUNT))
        result = pipeline.put_many(int, items)

        assert result is None
        assert float(value) in float_store.items

    float_store.items.clear()

    values = [random.uniform(-VALUES_MAX, VALUES_MAX) for _ in range(VALUES_COUNT)]

    for value in values:
        items = (value for _ in range(VALUES_COUNT))
        result = pipeline.put_many(float, items)

        assert result is None
        assert value in float_store.items

    float_store.items.clear()

    values = [str(random.uniform(-VALUES_MAX, VALUES_MAX)) for _ in range(VALUES_COUNT)]

    for value in values:
        items = (value for _ in range(VALUES_COUNT))
        result = pipeline.put_many(str, items)

        assert result is None
        assert float(value) in float_store.items
