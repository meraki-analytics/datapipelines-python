from abc import ABC, abstractmethod
from copy import deepcopy
from functools import singledispatch, update_wrapper
from typing import TypeVar, Type, Mapping, Any, Iterable, Callable, Union, AbstractSet

from merakicommons.cache import lazy_property

from .common import PipelineContext, UnsupportedError, NotFoundError, TYPE_WILDCARD

T = TypeVar("T")


class DataSource(ABC):
    @staticmethod
    def unsupported(type: Type[T]) -> UnsupportedError:
        return UnsupportedError("The type \"{type}\" is not supported by this DataSource!".format(type=type.__name__))

    @lazy_property
    def provides(self):  # type: Union[Iterable[Type[T]], Type[Any]]
        """The types of objects the data store provides."""
        types = set()
        any_dispatch = False
        try:
            types.update(getattr(self.__class__, "get")._provides)
            any_dispatch = True
        except AttributeError:
            pass
        try:
            types.update(getattr(self.__class__, "get_many")._provides)
            any_dispatch = True
        except AttributeError:
            pass
        return types if any_dispatch else TYPE_WILDCARD

    @abstractmethod
    def get(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> T:
        """Gets a query from the data source.

        Args:
            query: The query being requested.
            context: The context for the extraction (mutable).

        Returns:
            The requested object.
        """
        pass

    @abstractmethod
    def get_many(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        """Gets a query from the data source, which contains a request for multiple objects.

        Args:
            query: The query being requested (contains a request for multiple objects).
            context: The context for the extraction (mutable).

        Returns:
            The requested objects.
        """
        pass

    @staticmethod
    def dispatch(method: Callable[[Any, Type[T], Mapping[str, Any], PipelineContext], Any]) -> Callable[[Any, Type[T], Mapping[str, Any], PipelineContext], Any]:
        dispatcher = singledispatch(method)
        provides = set()

        def wrapper(self: Any, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> Any:
            call = dispatcher.dispatch(type)
            try:
                return call(self, query, context=context)
            except TypeError:
                raise DataSource.unsupported(type)

        def register(type: Type[T]) -> Callable[[Any, Type[T], Mapping[str, Any], PipelineContext], Any]:
            provides.add(type)
            return dispatcher.register(type)

        wrapper.register = register
        wrapper._provides = provides
        update_wrapper(wrapper, method)
        return wrapper


class CompositeDataSource(DataSource):
    def __init__(self, sources: Iterable[DataSource]) -> None:
        self._sources = {}
        for source in sources:
            for provided_type in source.provides:
                try:
                    providing_sources = self._sources[provided_type]
                except KeyError:
                    providing_sources = []
                    self._sources[provided_type] = providing_sources
                providing_sources.append(source)

    @property
    def provides(self) -> AbstractSet[Type]:
        return self._sources.keys()

    def get_many(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        try:
            sources = self._sources[type]
        except KeyError as error:
            raise DataSource.unsupported(type) from error

        for source in sources:
            try:
                return source.get_many(type, deepcopy(query), context)
            except NotFoundError:
                continue
        raise NotFoundError()

    def get(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> T:
        try:
            sources = self._sources[type]
        except KeyError as error:
            raise DataSource.unsupported(type) from error

        for source in sources:
            try:
                return source.get(type, deepcopy(query), context)
            except NotFoundError:
                continue
        raise NotFoundError()
