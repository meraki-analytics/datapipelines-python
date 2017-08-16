from abc import ABC, abstractmethod
from functools import singledispatch, update_wrapper
from typing import TypeVar, Type, Any, Iterable, Callable, Union, AbstractSet

from .common import PipelineContext, UnsupportedError, TYPE_WILDCARD

T = TypeVar("T")


class DataSink(ABC):
    @staticmethod
    def unsupported(type: Type[T]) -> UnsupportedError:
        return UnsupportedError("The type \"{type}\" is not supported by this DataSink!".format(type=type.__name__))

    @property
    def accepts(self):  # type: Union[Iterable[Type[T]], Type[Any]]
        """The types of objects the data sink can store."""
        types = set()
        any_dispatch = False
        try:
            types.update(getattr(self.__class__, "put")._accepts)
            any_dispatch = True
        except AttributeError:
            pass
        try:
            types.update(getattr(self.__class__, "put_many")._accepts)
            any_dispatch = True
        except AttributeError:
            pass
        return types if any_dispatch else TYPE_WILDCARD

    @abstractmethod
    def put(self, type: Type[T], item: T, context: PipelineContext = None) -> None:
        """Puts an object into the data sink.

        Args:
            type: The type of the object being inserted.
            item: The object to be inserted.
            context: The context of the insertion (mutable).
        """
        pass

    @abstractmethod
    def put_many(self, type: Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        """Puts multiple objects of the same type into the data sink.

        Args:
            type: The type of the objects being inserted.
            items: The objects to be inserted.
            context: The context of the insertion (mutable).
        """
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
        wrapper._accepts = accepts
        update_wrapper(wrapper, method)
        return wrapper


class CompositeDataSink(DataSink):
    def __init__(self, sinks: Iterable[DataSink]) -> None:
        self._sinks = {}
        for sink in sinks:
            for accepted_type in sink.accepts:
                try:
                    accepting_sinks = self._sinks[accepted_type]
                except KeyError:
                    accepting_sinks = set()
                    self._sinks[accepted_type] = accepting_sinks
                accepting_sinks.add(sink)

    @property
    def accepts(self) -> AbstractSet[Type]:
        return self._sinks.keys()

    def put_many(self, type: Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        try:
            sinks = self._sinks[type]
        except KeyError as error:
            raise DataSink.unsupported(type) from error

        items = list(items)
        for sink in sinks:
            sink.put_many(type, items, context)

    def put(self, type: Type[T], item: T, context: PipelineContext = None) -> None:
        try:
            sinks = self._sinks[type]
        except KeyError as error:
            raise DataSink.unsupported(type) from error

        for sink in sinks:
            sink.put(type, item, context)
