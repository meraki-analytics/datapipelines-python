import sys
from abc import ABC, abstractmethod
from enum import Enum
from copy import deepcopy
from typing import Type, MutableMapping, Any, Iterable, Union, Callable
from functools import wraps

from .pipelines import PipelineContext


class QueryValidationError(ValueError):
    pass


class MissingKeyError(QueryValidationError):
    pass


class WrongValueTypeError(QueryValidationError):
    pass


class BoundKeyExistenceError(QueryValidationError):
    pass


class QueryValidatorStructureError(AttributeError):
    pass


class _ValidationNode(ABC):
    @abstractmethod
    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> bool:
        pass

    @property
    @abstractmethod
    def falsifiable(self):
        pass


class _RootNode(_ValidationNode):
    def __init__(self, children: Iterable[Union["_KeyNode", "_AndNode", "_OrNode"]] = None) -> None:
        if children is None:
            children = list()
        self.children = list(children)

    def __str__(self) -> str:
        return " ALSO ".join(str(child) for child in self.children)

    @property
    def falsifiable(self):
        return False

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> True:
        # This always returns true because it'll raise a QueryValidationError if the query wasn't
        # valid. We also don't want normal AND behavior at this level so we just evaluate the
        # children and return True
        for child in self.children:
            child.evaluate(query, context)
        return True


class _AndNode(_ValidationNode):
    def __init__(self, children: Iterable[Union["_KeyNode", "_AndNode", "_OrNode"]]) -> None:
        self.children = list(children)

    def __str__(self) -> str:
        return " AND ".join(str(child) for child in self.children)

    @property
    def falsifiable(self):
        return False

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> True:
        # This is a little weird. We have to handle the case of can_have("x").and_("y") here.
        # Since the only type of Node that can return False rather than just raising a
        # QueryValidationError is a KeyNode that isn't required, or an OrNode whose children
        # are all KeyNodes that aren't required, we can't short circuit on a False value. If
        # we have can_have("x").and_("y"), and only some are True, we want raise a
        # QueryValidationError. If all are True or all are False, everything's fine.
        all_true = True
        all_possible_false = True
        for child in self.children:
            is_true = child.evaluate(query, context)
            all_true = all_true and is_true
            if child.falsifiable:
                all_possible_false = all_possible_false and not is_true

        if all_possible_false or all_true:
            return True
        raise BoundKeyExistenceError("Query must have all or none of the elements joined with \"and\" in a \"can_have\" statement!")


class _OrNode(_ValidationNode):
    def __init__(self, children: Iterable[Union["_KeyNode", "_AndNode", "_OrNode"]]) -> None:
        self.children = list(children)

    def __str__(self) -> str:
        return " OR ".join(str(child) for child in self.children)

    @property
    def falsifiable(self):
        return True

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> bool:
        # We can't short circuit this because we want to raise any WrongValueTypeError or BoundKeyExistenceError that could occur.
        # We count failures because the only other node than can return False is a KeyNode that isn't required. An OrNode is only
        # allowed to return False when all its children are False KeyNodes. The AndNodes handle raising an error if a can_have
        # expression with ands and ors fails.
        errors = []
        failure_count = 0
        result = False
        for child in self.children:
            try:
                is_true = child.evaluate(query, context)
                if not is_true:
                    failure_count += 1
                result = is_true or result
            except (MissingKeyError, BoundKeyExistenceError) as error:
                errors.append(error)
                failure_count += 1
        if failure_count == len(self.children) and len(errors) > 0:
            raise errors[0]
        return result


class _DefaultValueNode(_ValidationNode):
    def __init__(self, key: str, value: Union[Any, Callable[[MutableMapping[str, Any]], Any]], supplies_type: Type = None) -> None:
        self.key = key
        self.value = value
        self.supplies_type = supplies_type

    def __str__(self) -> str:
        return self.value

    @property
    def falsifiable(self):
        return False

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> True:
        if self.supplies_type:
            query[self.key] = self.value(query, context)
        else:
            query[self.key] = deepcopy(self.value)
        return True


class _TypeNode(_ValidationNode):
    def __init__(self, key: str, types: Iterable[Type], child: _DefaultValueNode = None) -> None:
        self.key = key
        self.child = child
        self.types = types

    def __str__(self) -> str:
        return " OR ".join(type.__name__ for type in self.types)

    @property
    def falsifiable(self):
        return False

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> True:
        try:
            value = query[self.key]
            for type in self.types:
                if sys.version_info.minor >= 7 and hasattr(type, '__origin__'):  # The typing module contains a reference to the actual type via __origin__. Get that actual type for use in `issubclass`. This only works for python 3.7+, not 3.6 or below.
                    type = type.__origin__
                if issubclass(type, Enum) and isinstance(value, str):
                    value = type(value)
                if isinstance(value, type):
                    query[self.key] = value
                    return True
            raise WrongValueTypeError("{key} must be of type {type} in query! Got {badtype}.".format(key=self.key, type=self, badtype=type(value)))
        except KeyError:
            if self.child:
                self.child.evaluate(query, context)
        return True


class _KeyNode(_ValidationNode):
    def __init__(self, key: str, required: bool = True, child: _TypeNode = None) -> None:
        self.key = key
        self.required = required
        self.child = child

    def __str__(self) -> str:
        return self.key

    @property
    def falsifiable(self):
        return not self.required

    def evaluate(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> bool:
        has_key = self.key in query
        if self.required and not has_key:
            raise MissingKeyError("{key} must be in query!".format(key=self.key))
        if self.child:
            self.child.evaluate(query, context)
        return has_key


class QueryValidator(object):
    def __init__(self) -> None:
        self._root = _RootNode()
        self._current = None  # type: _KeyNode
        self._parent = None  # type: Union[_AndNode, _OrNode]

    def __call__(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> True:
        return self._root.evaluate(query, context)

    def has(self, key: str) -> "QueryValidator":
        if self._current is not None:
            raise QueryValidatorStructureError("A key is already selected! Try using \"also\" before \"has\".")

        has_node = _KeyNode(key, True)
        self._root.children.append(has_node)
        self._current = has_node
        self._parent = self._root
        return self

    def can_have(self, key: str) -> "QueryValidator":
        if self._current is not None:
            raise QueryValidatorStructureError("A key is already selected! Try using \"also\" before \"can_have\".")

        has_node = _KeyNode(key, False)
        self._root.children.append(has_node)
        self._current = has_node
        self._parent = self._root
        return self

    def as_(self, type: Type) -> "QueryValidator":
        if self._current is None or self._current.child is not None:
            raise QueryValidatorStructureError("No key is selected! Try using \"has\" or \"can_have\" before \"as\".")

        type_node = _TypeNode(self._current.key, {type})
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
            self._parent.children.append(or_node)
            self._parent = or_node

        has_node = _KeyNode(key, self._current.required)
        or_node.children.append(has_node)
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
            self._parent.children.append(and_node)
            self._parent = and_node

        has_node = _KeyNode(key, self._current.required)
        and_node.children.append(has_node)
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


def validate_query(validator: QueryValidator, *pre_transforms: Callable[[MutableMapping], None]) -> Callable[[Callable[[Any, MutableMapping[str, Any], PipelineContext], Union[Any, Iterable[Any]]]], Callable[[Any, MutableMapping[str, Any], PipelineContext], Union[Any, Iterable[Any]]]]:
    def wrapper(method: Callable[[Any, MutableMapping[str, Any], PipelineContext], Union[Any, Iterable[Any]]]) -> Callable[[Any, MutableMapping[str, Any], PipelineContext], Union[Any, Iterable[Any]]]:
        @wraps(method)
        def wrapped(self: Any, query: MutableMapping[str, Any], context: PipelineContext = None):
            for transform in pre_transforms:
                transform(query)

            validator(query)
            return method(self, query, context)

        return wrapped
    return wrapper
