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
        types = set()
        any_dispatch = False
        try:
            types.update(getattr(self.__class__, "get").__provides)
            any_dispatch = True
        except AttributeError:
            pass
        try:
            types.update(getattr(self.__class__, "get_many").__provides)
            any_dispatch = True
        except AttributeError:
            pass
        return types if any_dispatch else TYPE_WILDCARD

    @abstractmethod
    def get(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> T:
        pass

    @abstractmethod
    def get_many(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
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
        wrapper.__provides = provides
        update_wrapper(wrapper, method)
        return wrapper
