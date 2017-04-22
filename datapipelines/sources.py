from abc import ABC, abstractmethod
from functools import singledispatch, update_wrapper
from typing import TypeVar, Type, Mapping, Any, Iterable, Callable, Collection, Union

from .common import PipelineContext, UnsupportedError, TYPE_WILDCARD

T = TypeVar("T")


class DataSource(ABC):
    @staticmethod
    def unsupported(type: Type[T]) -> UnsupportedError:
        return UnsupportedError("The type \"{type}\" is not supported by this DataSource!".format(type=type.__name__))

    @property
    def provides(self) -> Union[Collection[Type[T]], Type[Any]]:
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
