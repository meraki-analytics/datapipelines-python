from abc import ABC, abstractmethod
from functools import singledispatch, update_wrapper
from typing import TypeVar, Type, Callable, Any, Mapping, Iterable, Dict, Set

from merakicommons.cache import lazy_property

from .common import PipelineContext, UnsupportedError, TypePair

F = TypeVar("F")
T = TypeVar("T")


class DataTransformer(ABC):
    @staticmethod
    def unsupported(target_type: Type[T], value: F) -> UnsupportedError:
        return UnsupportedError("The conversion from type \"{from_type}\" to type \"{to_type}\" is not supported by this DataTransformer!".format(from_type=value.__class__.__name__, to_type=target_type.__name__))

    @property
    def transforms(self) -> Mapping[Type, Iterable[Type]]:
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

        dispatch_types = {}

        def wrapper(self: Any, target_type: Type[T], value: F, context: PipelineContext = None) -> T:
            dispatch_type_name = f"{value.__class__.__name__} -> {target_type.__name__}"
            dispatch_type = dispatch_types[dispatch_type_name]
            call = dispatcher.dispatch(dispatch_type)
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

            dispatch_type_name = f'{from_type.__name__} -> {to_type.__name__}'
            new_type = type(dispatch_type_name, (object,), {})
            dispatch_types[dispatch_type_name] = new_type
            return dispatcher.register(new_type)

        wrapper.register = register
        wrapper._transforms = transforms
        update_wrapper(wrapper, method)
        return wrapper


class CompositeDataTransformer(DataTransformer):
    def __init__(self, transformers: Iterable[DataTransformer]) -> None:
        self._transformers = {}
        for transformer in transformers:
            for source, targets in transformer.transforms.items():
                for target in targets:
                    try:
                        current_transformer = self._transformers[(source, target)]
                        if transformer.cost < current_transformer.cost:
                            self._transformers[(source, target)] = transformer
                    except KeyError:
                        self._transformers[(source, target)] = transformer

    @lazy_property
    def transforms(self) -> Dict[Type, Set[Type]]:
        transforms = {}
        for source, target in self._transformers:
            try:
                current = transforms[source]
            except KeyError:
                current = set()
                transforms[source] = current
            current.add(target)
        return transforms

    @lazy_property
    def cost(self) -> int:
        return max(transformer.cost for transformer in self._transformers.values())

    def transform(self, target_type: Type[T], value: F, context: PipelineContext = None) -> T:
        try:
            transformer = self._transformers[type(value), target_type]
        except KeyError as error:
            raise DataTransformer.unsupported(target_type, value) from error

        return transformer.transform(target_type, value, context)
