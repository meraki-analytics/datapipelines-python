from abc import ABC, abstractmethod
from typing import Type, MutableMapping, Any, Iterable, Union, Callable
from enum import Enum

from .pipelines import PipelineContext


class QueryValidationError(ValueError):
    pass


class MissingKeyError(QueryValidationError):
    pass


class WrongValueTypeError(QueryValidationError):
    pass


class QueryValidatorStructureError(AttributeError):
    pass


class _ValidationNode(ABC):
    @abstractmethod
    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> bool:
        pass


class _AndNode(_ValidationNode):
    def __init__(self, children: Iterable[Union["_KeyNode", "_AndNode", "_OrNode"]]) -> None:
        self.children = set(children)

    def __str__(self) -> str:
        return " AND ".join(str(child) for child in self.children)

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> bool:
        return all(child.evaluate(query, context) for child in self.children)


class _OrNode(_ValidationNode):
    def __init__(self, children: Iterable[Union["_KeyNode", "_AndNode", "_OrNode"]]) -> None:
        self.children = set(children)

    def __str__(self) -> str:
        return " OR ".join(str(child) for child in self.children)

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> bool:
        # We can't short circuit this because we want to raise any WrongValueTypeError that could occur
        errors = []
        result = False
        for child in self.children:
            try:
                result = child.evaluate(query, context) or result
            except MissingKeyError as error:
                errors.append(error)
        if len(errors) == len(self.children):
            raise errors[0]
        return result


class _DefaultValueNode(_ValidationNode):
    def __init__(self, key: str, value: Union[Any, Callable[[MutableMapping[str, Any]], Any]], supplies_type: Type = None) -> None:
        self.key = key
        self.value = value
        self.supplies_type = supplies_type

    def __str__(self) -> str:
        return self.value

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> bool:
        if self.supplies_type:
            query[self.key] = self.value(query, context)
        else:
            query[self.key] = self.value
        return True


class _TypeNode(_ValidationNode):
    def __init__(self, key: str, types: Union[Type, Iterable[Type]], child: _DefaultValueNode = None) -> None:
        self.key = key
        self.child = child
        self.types = set(types) if isinstance(types, Iterable) else {types}

    def __str__(self) -> str:
        return " OR ".join(type.__name__ for type in self.types)

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> bool:
        try:
            value = query[self.key]
            for type in self.types:
                if issubclass(type, Enum) and isinstance(value, str):
                    value = type(value)
                if isinstance(value, type):
                    return True
            raise WrongValueTypeError("{key} must be of type {type} in query!".format(key=self.key, type=self))
        except KeyError:
            if self.child:
                return self.child.evaluate(query, context)
        return True


class _KeyNode(_ValidationNode):
    def __init__(self, key: str, required: bool = True, child: Union[_TypeNode, _OrNode, _AndNode] = None) -> None:
        self.key = key
        self.required = required
        self.child = child

    def __str__(self) -> str:
        return self.key

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> bool:
        if self.required and self.key not in query:
            raise MissingKeyError("{key} must be in query!".format(key=self.key))
        if self.child:
            return self.child.evaluate(query, context)
        return True


class QueryValidator(object):
    def __init__(self) -> None:
        self._root = _AndNode(set())
        self._current = None  # type: _KeyNode
        self._parent = None  # type: Union[_AndNode, _OrNode]

    def __call__(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> bool:
        return self._root.evaluate(query, context)

    def has(self, key: str) -> "QueryValidator":
        if self._current is not None:
            raise QueryValidatorStructureError("A key is already selected! Try using \"also\" before \"has\".")

        has_node = _KeyNode(key, True)
        self._root.children.add(has_node)
        self._current = has_node
        self._parent = self._root
        return self

    def can_have(self, key: str) -> "QueryValidator":
        if self._current is not None:
            raise QueryValidatorStructureError("A key is already selected! Try using \"also\" before \"can_have\".")

        has_node = _KeyNode(key, False)
        self._root.children.add(has_node)
        self._current = has_node
        self._parent = self._root
        return self

    def as_(self, type: Type) -> "QueryValidator":
        if self._current is None or self._current.child is not None:
            raise QueryValidatorStructureError("No key is selected! Try using \"has\" or \"can_have\" before \"as\".")

        type_node = _TypeNode(self._current.key, type)
        self._current.child = type_node
        return self

    def as_any_of(self, types: Iterable[Type]) -> "QueryValidator":
        if self._current is None or self._current.child is not None:
            raise QueryValidatorStructureError("No key is selected! Try using \"has\" or \"can_have\" before \"as_any_of\".")

        type_node = _TypeNode(self._current.key, types)
        self._current.child = type_node
        return self

    def or_(self, key: str) -> "QueryValidator":
        if self._current is None:
            raise QueryValidatorStructureError("No key is selected! Try using \"has\" or \"can_have\" before \"or\".")

        if isinstance(self._parent, _OrNode):
            or_node = self._parent
        else:
            or_node = _OrNode({self._current})
            self._parent.children.remove(self._current)
            self._parent.children.add(or_node)
            self._parent = or_node

        has_node = _KeyNode(key, self._current.required)
        or_node.children.add(has_node)
        self._current = has_node
        return self

    def and_(self, key: str) -> "QueryValidator":
        if self._current is None:
            raise QueryValidatorStructureError("No key is selected! Try using \"has\" or \"can_have\" before \"and\".")

        if isinstance(self._parent, _AndNode):
            and_node = self._parent
        else:
            and_node = _AndNode({self._current})
            self._parent.children.remove(self._current)
            self._parent.children.add(and_node)
            self._parent = and_node

        has_node = _KeyNode(key, self._current.required)
        and_node.children.add(has_node)
        self._current = has_node
        return self

    @property
    def also(self) -> "QueryValidator":
        if self._current is None:
            raise QueryValidatorStructureError("No key is selected! Try using \"has\" or \"can_have\" before \"also\".")

        self._current = None
        self._parent = None
        return self

    def with_default(self, value: Union[Any, Callable[[MutableMapping[str, Any]], Any]], supplies_type: Type = None) -> "QueryValidator":
        if self._current is None or self._current.child is not None:
            raise QueryValidatorStructureError("No key is selected! Try using \"can_have\" before \"with_default\".")

        if self._current.required:
            raise QueryValidatorStructureError("Can't assign a default value to a required key! Try using \"can_have\" instead of \"have\".")

        if supplies_type:
            expected_type = supplies_type
        else:
            expected_type = type(value)

        default_node = _DefaultValueNode(self._current.key, value, supplies_type)
        result = self.as_(expected_type)
        result._current.child.child = default_node
        return result


class Query(dict):
    @staticmethod
    def has(key: str) -> QueryValidator:
        return QueryValidator().has(key)

    @staticmethod
    def can_have(key: str) -> QueryValidator:
        return QueryValidator().can_have(key)
