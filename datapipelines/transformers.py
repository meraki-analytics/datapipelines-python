from abc import ABC, abstractmethod
from functools import singledispatch, update_wrapper
from typing import TypeVar, Type, Callable, Any, Mapping, Collection

from .common import PipelineContext, UnsupportedError, TypePair

F = TypeVar("F")
T = TypeVar("T")


class DataTransformer(ABC):
    @staticmethod
    def unsupported(target_type: Type[T], value: F) -> UnsupportedError:
        return UnsupportedError("The conversion from type \"{from_type}\" to type \"{to_type}\" is not supported by this DataTransformer!".format(from_type=value.__class__.__name__, to_type=target_type.__name__))

    @property
    def transforms(self) -> Mapping[Type, Collection[Type]]:
        """The available data transformers."""
        try:
            return getattr(self.__class__, "transform")._transforms
        except AttributeError:
            return {}

    @abstractmethod
    def transform(self, target_type: Type[T], value: F, context: PipelineContext = None) -> T:
        """Transforms an object to a new type.

        Args:
            target_type: The type to be converted to.
            value: The object to be transformed.
            context: The context of the transformation (mutable).
        """
        pass

    @property
    def cost(self) -> int:
        """The cost of the tranformation (default 1)."""
        return 1

    @staticmethod
    def dispatch(method: Callable[[Any, Type[T], F, PipelineContext], T]) -> Callable[[Any, Type[T], F, PipelineContext], T]:
        dispatcher = singledispatch(method)
        transforms = {}

        def wrapper(self: Any, target_type: Type[T], value: F, context: PipelineContext = None) -> T:
            call = dispatcher.dispatch(TypePair[value.__class__, target_type])
            try:
                return call(self, value, context=context)
            except TypeError:
                raise DataTransformer.unsupported(target_type, value)

        def register(from_type: Type[F], to_type: Type[T]) -> Callable[[Any, Type[T], F, PipelineContext], T]:
            try:
                target_types = transforms[from_type]
            except KeyError:
                target_types = set()
                transforms[from_type] = target_types
            target_types.add(to_type)

            return dispatcher.register(TypePair[from_type, to_type])

        wrapper.register = register
        wrapper._transforms = transforms
        update_wrapper(wrapper, method)
        return wrapper
