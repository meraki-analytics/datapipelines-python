from abc import ABC, abstractmethod
from functools import singledispatch, update_wrapper
from typing import TypeVar, Type, Any, Iterable, Collection, Callable, Union

from .common import PipelineContext, UnsupportedError, TYPE_WILDCARD

T = TypeVar("T")


class DataSink(ABC):
    @staticmethod
    def unsupported(type: Type[T]) -> UnsupportedError:
        return UnsupportedError("The type \"{type}\" is not supported by this DataSink!".format(type=type.__name__))

    @property
    def accepts(self) -> Union[Collection[Type[T]], Type[Any]]:
        types = set()
        any_dispatch = False
        try:
            types.update(getattr(self.__class__, "put").__accepts)
            any_dispatch = True
        except AttributeError:
            pass
        try:
            types.update(getattr(self.__class__, "put_many").__accepts)
            any_dispatch = True
        except AttributeError:
            pass
        return types if any_dispatch else TYPE_WILDCARD

    @abstractmethod
    def put(self, type: Type[T], item: T, context: PipelineContext = None) -> None:
        pass

    @abstractmethod
    def put_many(self, type: Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        pass

    @staticmethod
    def dispatch(method: Callable[[Any, Type[T], Any, PipelineContext], None]) -> Callable[[Any, Type[T], Any, PipelineContext], None]:
        dispatcher = singledispatch(method)
        accepts = set()

        def wrapper(self: Any, type: Type[T], items: Any, context: PipelineContext = None) -> None:
            call = dispatcher.dispatch(type)
            try:
                return call(self, items, context=context)
            except TypeError:
                raise DataSink.unsupported(type)

        def register(type: Type[T]) -> Callable[[Any, Type[T], Any, PipelineContext], None]:
            accepts.add(type)
            return dispatcher.register(type)

        wrapper.register = register
        wrapper.__accepts = accepts
        update_wrapper(wrapper, method)
        return wrapper
