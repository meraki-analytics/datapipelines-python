from typing import Type, TypeVar, Sequence, Union, Collection, Optional, Callable, Any, List, Set, Generic, Mapping, Iterable, Tuple, Generator
from functools import partial
from itertools import tee
from logging import getLogger

from networkx import DiGraph, dijkstra_path, NetworkXNoPath

from .transformers import DataTransformer
from .common import PipelineContext, NotFoundError, TYPE_WILDCARD
from .sources import DataSource
from .sinks import DataSink

LOGGER = getLogger(__name__)

_SOURCES = "sources"
_SINKS = "sinks"
_TRANSFORMER = "transformer"

_MAX_TRANSFORM_COST = 10000000  # (つ͡°͜ʖ͡°)つ


def _build_type_graph(sources: Collection[DataSource], sinks: Collection[DataSink], transformers: Collection[DataTransformer]) -> DiGraph:
    graph = DiGraph()

    for source in sources:
        # We ignore wildcard sources in the graph since they won't require a search to determine whether they can provide a type
        if TYPE_WILDCARD is source.provides:
            LOGGER.info("Ignoring \"{source}\" in type graph, as it provides the wildcard type".format(source=source))
            continue

        provides = source.provides  # type: Collection[Type]
        for provided_type in provides:
            try:
                provided_type_sources = graph[provided_type][_SOURCES]
            except KeyError:
                provided_type_sources = set()

            provided_type_sources.add(source)
            graph.add_node(provided_type, sources=provided_type_sources)
        LOGGER.info("Added source \"{source}\" to type graph".format(source=source))

    for sink in sinks:
        # We ignore wildcard sinks in the graph since they won't require a search to determine whether they can accept a type
        if TYPE_WILDCARD is sink.accepts:
            LOGGER.info("Ignoring \"{sink}\" in type graph, as it accepts the wildcard type".format(sink=sink))
            continue
        else:
            accepts = sink.accepts  # type: Collection[Type]

        for accepted_type in accepts:
            try:
                accepted_type_sinks = graph[accepted_type][_SINKS]
            except KeyError:
                accepted_type_sinks = set()

            accepted_type_sinks.add(sink)
            graph.add_node(accepted_type, sinks=accepted_type_sinks)
        LOGGER.info("Added sink \"{sink}\" to type graph".format(sink=sink))

    for transformer in transformers:
        # A wildcard transformer would be ridiculous so those don't exist
        for from_type, to_types in transformer.transforms.items():
            for to_type in to_types:
                # Add the type nodes into the graph if they aren't there already
                graph.add_node(from_type)
                graph.add_node(to_type)

                # Only the cheapest conversion between two types is stored. Replace it if this one is cheaper.
                try:
                    current_transformer = graph.edge[from_type][to_type][_TRANSFORMER]
                    cheapest_transformer = transformer if transformer.cost < current_transformer.cost else current_transformer
                except KeyError:
                    cheapest_transformer = transformer

                graph.add_edge(from_type, to_type, cost=transformer.cost, transformer=cheapest_transformer)
        LOGGER.info("Added transformer \"{transformer}\" to type graph".format(transformer=transformer))

    return graph


class NoConversionError(ValueError):
    pass


T = TypeVar("T")
S = TypeVar("S")


def _pairwise(iterable: Iterable[Any]) -> Iterable[Tuple[Any, Any]]:
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def _identity(data: T, context: PipelineContext = None) -> T:
    return data


def _transform(transformer_chain: Sequence[Tuple[DataTransformer, Type]], data: S, context: PipelineContext = None) -> T:
    for transformer, target_type in transformer_chain:
        # noinspection PyTypeChecker
        data = transformer.transform(target_type, data, context)
    return data


class _SinkHandler(Generic[S, T]):
    def __init__(self, sink: DataSink, source_type: Type[S], transform: Callable[[T], S]) -> None:
        self._sink = sink
        self._source_type = source_type
        self._transform = transform

    def put(self, item: T, context: PipelineContext = None) -> None:
        LOGGER.info("Converting item \"{item}\" for sink \"{sink}\"".format(item=item, sink=self._sink))
        item = self._transform(data=item, context=context)
        LOGGER.info("Puting item \"{item}\" into sink \"{sink}\"".format(item=item, sink=self._sink))
        self._sink.put(self._source_type, item, context)

    def put_many(self, items: Iterable[T], context: PipelineContext = None) -> None:
        LOGGER.info("Creating transform generator for items \"{items}\" for sink \"{sink}\"".format(items=items, sink=self._sink))
        transform_generator = (self._transform(data=item, context=context) for item in items)
        LOGGER.info("Putting transform generator for items \"{items}\" into sink \"{sink}\"".format(items=items, sink=self._sink))
        self._sink.put_many(self._source_type, transform_generator, context)


class _SourceHandler(Generic[S, T]):
    def __init__(self, source: DataSource, source_type: Type[S], transform: Callable[[S], T], sinks: Mapping[_SinkHandler, bool]) -> None:
        self._source = source
        self._source_type = source_type
        self._transform = transform
        self._before_transform = {sink for sink, do_transform in sinks.items() if not do_transform}
        self._after_transform = {sink for sink, do_transform in sinks.items() if do_transform}

    def get(self, query: Mapping[str, Any], context: PipelineContext = None) -> T:
        result = self._source.get(self._source_type, query, context)
        LOGGER.info("Got result \"{result}\" from query \"{query}\" of source \"{source}\"".format(result=result, query=query, source=self._source))

        LOGGER.info("Sending result \"{result}\" to sinks before converting".format(result=result))
        for sink in self._before_transform:
            sink.put(result, context)

        LOGGER.info("Converting result \"{result}\" to request type".format(result=result))
        result = self._transform(data=result, context=context)

        LOGGER.info("Sending result \"{result}\" to sinks after converting".format(result=result))
        for sink in self._after_transform:
            sink.put(result, context)

        return result

    def __get_many_generator(self, result: Iterable[S], context: PipelineContext = None) -> Generator[T, None, None]:
        for item in result:
            LOGGER.info("Sending item \"{item}\" to sinks before converting".format(item=item))
            for sink in self._before_transform:
                sink.put(item, context)

            LOGGER.info("Converting item \"{item}\" to request type".format(item=item))
            item = self._transform(data=item, context=context)

            LOGGER.info("Sending item \"{item}\" to sinks after converting".format(item=item))
            for sink in self._after_transform:
                sink.put(item, context)

            yield item

    def get_many(self, query: Mapping[str, Any], context: PipelineContext = None, streaming: bool = True) -> Iterable[T]:
        result = self._source.get_many(self._source_type, query, context)
        LOGGER.info("Got results \"{result}\" from query \"{query}\" of source \"{source}\"".format(result=result, query=query, source=self._source))

        if not streaming:
            LOGGER.info("Non-streaming get_many request. Ensuring results \"{result}\" are a Collection".format(result=result))
            result = list(result)

            LOGGER.info("Sending results \"{result}\" to sinks before converting".format(result=result))
            for sink in self._before_transform:
                sink.put_many(result, context)

            LOGGER.info("Converting results \"{result}\" to request type".format(result=result))
            result = [self._transform(data=item, context=context) for item in result]

            LOGGER.info("Sending results \"{result}\" to sinks after converting".format(result=result))
            for sink in self._after_transform:
                sink.put_many(result, context)

            return result
        else:
            LOGGER.info("Streaming get_many request. Returning result generator for results \"{result}\"".format(result=result))
            return self.__get_many_generator(result)


class DataPipeline(object):
    def __init__(self, elements: Sequence[Union[DataSource, DataSink]], transformers: Collection[DataTransformer] = None) -> None:
        if not elements:
            raise ValueError("Elements must be a non-empty sequence of DataSources and DataSinks")

        if transformers is None:
            transformers = set()

        sources = set()  # type: Set[DataSource]
        sinks = set()  # type: Set[DataSink]
        targets = []  # type: List[Tuple[DataSource, Set[DataSink]]]
        for element in elements:
            if isinstance(element, DataSource):
                sources.add(element)
                targets.append((element, set(sinks)))

            if isinstance(element, DataSink):
                sinks.add(element)

        LOGGER.info("Beginning construction of type graph")
        # noinspection PyTypeChecker
        self.__type_graph = _build_type_graph(sources, sinks, transformers)
        LOGGER.info("Completed construction of type graph")
        self.__sources = targets
        self.__sinks = sinks
        self.__get_types = {}
        self.__put_types = {}

    def __transform(self, source_type: Type[S], target_type: Type[T]) -> Tuple[Callable[[S], T], int]:
        LOGGER.info("Searching type graph for shortest path from \"{source_type}\" to \"{target_type}\"".format(source_type=source_type.__name__, target_type=target_type.__name__))
        path = dijkstra_path(self.__type_graph, source=source_type, target=target_type)
        LOGGER.info("Found a path from \"{source_type}\" to \"{target_type}\"".format(source_type=source_type.__name__, target_type=target_type.__name__))

        LOGGER.info("Building transformer chain from \"{source_type}\" to \"{target_type}\"".format(source_type=source_type.__name__, target_type=target_type.__name__))
        chain = []
        cost = 0
        for source, target in _pairwise(path):
            transformer = self.__type_graph.edge[source][target][_TRANSFORMER]
            chain.append((transformer, target))
            cost += transformer.cost
        LOGGER.info("Built transformer chain from \"{source_type}\" to \"{target_type}\"".format(source_type=source_type.__name__, target_type=target_type.__name__))

        return partial(_transform, transformer_chain=chain), cost

    def __sink_handler(self, sink: DataSink, *target_types: Type) -> Tuple[_SinkHandler, int]:
        for index, target_type in enumerate(target_types):
            if TYPE_WILDCARD is sink.accepts or target_type in sink.accepts:
                LOGGER.info("Sink \"{sink}\" accepts \"{target_type}\" directly".format(sink=sink, target_type=target_type.__name__))
                # noinspection PyTypeChecker
                return _SinkHandler(sink, target_type, _identity), index

        transform = None
        cost = _MAX_TRANSFORM_COST
        type_index = -1

        accepts = sink.accepts  # type: Collection[Type]
        for index, target_type in enumerate(target_types):
            LOGGER.info("Attempting to find a transformer chain from \"{target_type}\" to a type sink \"{sink}\" accepts".format(target_type=target_type.__name__, sink=sink))
            for accepted_type in accepts:
                try:
                    # noinspection PyTypeChecker
                    t, c = self.__transform(target_type, accepted_type)
                    LOGGER.info("Found a transformer chain from \"{target_type}\" to \"{accepted_type}\" at cost {cost}".format(target_type=target_type.__name__, accepted_type=accepted_type.__name__, cost=c))
                    if c < cost:
                        transform = t
                        cost = c
                        type_index = index
                except NetworkXNoPath:
                    pass

        if transform is None:
            names = [target_type.__name__ for target_type in target_types]
            raise NoConversionError("Sink can't accept any of {names}!".format(names=names))

        LOGGER.info("Taking cheapest transformer chain for sink \"{sink}\" (from \"{target_type}\") at cost {cost}".format(sink=sink, target_type=target_types[type_index].__name__, cost=cost))

        return _SinkHandler(sink, target_types[type_index], transform), type_index

    def __source_handler(self, source: DataSource, sinks: Collection[DataSink], target_type: Type[T]) -> _SourceHandler:
        if TYPE_WILDCARD is source.provides or target_type in source.provides:
            LOGGER.info("Source \"{source}\" provides \"{target_type}\" directly".format(source=source, target_type=target_type.__name__))
            transform = _identity
            source_type = target_type
        else:
            transform = None
            cost = _MAX_TRANSFORM_COST
            source_type = None

            LOGGER.info("Attempting to find a transformer chain to \"{target_type}\" from a type source \"{source}\" provides".format(target_type=target_type.__name__, source=source))
            for provided_type in source.provides:
                try:
                    t, c = self.__transform(provided_type, target_type)
                    LOGGER.info("Found a transformer chain from \"{provided_type}\" to \"{target_type}\" at cost {cost}".format(provided_type=provided_type.__name__, target_type=target_type.__name__, cost=c))
                    if c < cost:
                        transform = t
                        cost = c
                        source_type = provided_type
                except NetworkXNoPath:
                    pass

            if transform is None:
                raise NoConversionError("Source can't provide \"{target_type}\"!".format(target_type=target_type.__name__))

            LOGGER.info("Taking cheapest transformer chain for source \"{source}\" (to \"{target_type}\") at cost {cost}".format(source=source, target_type=target_type.__name__, cost=cost))

        LOGGER.info("Building SinkHandlers for source \"{source}\" for target type \"{target_type}\"".format(source=source, target_type=target_type.__name__))
        target_sinks = {}
        for sink in sinks:
            try:
                handler, do_transform = self.__sink_handler(sink, source_type, target_type)
                target_sinks[handler] = do_transform
            except NoConversionError:
                pass
        LOGGER.info("Built SinkHandlers for source \"{source}\" for target type \"{target_type}\"".format(source=source, target_type=target_type.__name__))

        return _SourceHandler(source, source_type, transform, target_sinks)

    def __get_handlers(self, type: Type[T]) -> List[_SourceHandler]:
        handlers = []  # type: List[_SourceHandler]

        for source, sinks in self.__sources:
            try:
                handler = self.__source_handler(source, sinks, type)
                handlers.append(handler)
            except NoConversionError:
                pass

        if not handlers:
            raise NoConversionError("No source provides \"{type}\"".format(type=type.__name__))

        return handlers

    def __put_handlers(self, type: Type[T]) -> Set[_SinkHandler]:
        handlers = set()

        for sink in self.__sinks:
            try:
                handler = self.__sink_handler(sink, type)
                handlers.add(handler)
            except NoConversionError:
                pass

        if not handlers:
            raise NoConversionError("No sink accepts \"{type}\"".format(type=type.__name__))

        return handlers

    def __new_context(self) -> PipelineContext:
        context = PipelineContext()
        context[PipelineContext.Keys.PIPELINE] = self
        return context

    def get(self, type: Type[T], query: Mapping[str, Any]) -> T:
        LOGGER.info("Getting SourceHandlers for \"{type}\"".format(type=type.__name__))
        try:
            handlers = self.__get_types[type]
        except KeyError:
            try:
                LOGGER.info("Building new SourceHandlers for \"{type}\"".format(type=type.__name__))
                handlers = self.__get_handlers(type)
            except NoConversionError:
                handlers = None
            self.__get_types[type] = handlers

        if handlers is None:
            raise NoConversionError("No source can provide \"{type}\"".format(type=type.__name__))

        LOGGER.info("Creating new PipelineContext")
        context = self.__new_context()

        LOGGER.info("Querying SourceHandlers for \"{type}\"".format(type=type.__name__))
        for handler in handlers:
            try:
                return handler.get(query, context)
            except NotFoundError:
                pass

        raise NotFoundError("No source returned a query result!")

    def get_many(self, type: Type[T], query: Mapping[str, Any], streaming: bool = True) -> Iterable[T]:
        LOGGER.info("Getting SourceHandlers for \"{type}\"".format(type=type.__name__))
        try:
            handlers = self.__get_types[type]
        except KeyError:
            try:
                LOGGER.info("Building new SourceHandlers for \"{type}\"".format(type=type.__name__))
                handlers = self.__get_handlers(type)
            except NoConversionError:
                handlers = None
            self.__get_types[type] = handlers

        if handlers is None:
            raise NoConversionError("No source can provide \"{type}\"".format(type=type.__name__))

        LOGGER.info("Creating new PipelineContext")
        context = self.__new_context()

        LOGGER.info("Querying SourceHandlers for \"{type}\"".format(type=type.__name__))
        for handler in handlers:
            try:
                return handler.get_many(query, context, streaming)
            except NotFoundError:
                pass

        raise NotFoundError("No source returned a query result!")

    def put(self, type: Type[T], item: T) -> None:
        LOGGER.info("Getting SinkHandlers for \"{type}\"".format(type=type.__name__))
        try:
            handlers = self.__put_types[type]
        except KeyError:
            try:
                LOGGER.info("Building new SinkHandlers for \"{type}\"".format(type=type.__name__))
                handlers = self.__put_handlers(type)
            except NoConversionError:
                handlers = None
            self.__get_types[type] = handlers

        LOGGER.info("Creating new PipelineContext")
        context = self.__new_context()

        LOGGER.info("Sending item \"{item}\" to SourceHandlers".format(item=item))
        if handlers is not None:
            for handler in handlers:
                handler.put(item, context)

    def put_many(self, type: Type[T], items: Iterable[T]) -> None:
        LOGGER.info("Getting SinkHandlers for \"{type}\"".format(type=type.__name__))
        try:
            handlers = self.__put_types[type]
        except KeyError:
            try:
                LOGGER.info("Building new SinkHandlers for \"{type}\"".format(type=type.__name__))
                handlers = self.__put_handlers(type)
            except NoConversionError:
                handlers = None
            self.__get_types[type] = handlers

        LOGGER.info("Creating new PipelineContext")
        context = self.__new_context()

        LOGGER.info("Sending items \"{items}\" to SourceHandlers".format(items=items))
        if handlers is not None:
            items = list(items)
            for handler in handlers:
                handler.put_many(items, context)