# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""
This module defines event types and functions used by `Node._op_walk()` and
`Node._get_node_descriptor()`.
"""
from __future__ import annotations

import collections
import inspect
import pathlib

from . import (
    gn,
    grun,
    nodeop,
    ty,
)

# The types below are used by `Node._op_walk()` and
# `Node._get_node_descriptor()`
class ValueId(ty.NamedTuple):
    type: str
    id: int

class Ref(ty.NamedTuple):
    type: str
    id: int

class OpType(ty.NamedTuple):
    type: str
    value: ty.Type[nodeop.NodeOp]

class DataOp(ty.NamedTuple):
    type: str
    value: nodeop.NodeOp

class PathOut(ty.NamedTuple):
    type: str
    value: nodeop.PathOut

class PathIn(ty.NamedTuple):
    type: str
    value: nodeop.PathIn

class Node(ty.NamedTuple):
    type: str
    value: ty.Union[gn.Node[ty.Any], None]

class CTX(ty.NamedTuple):
    type: str

class UNSET(ty.NamedTuple):
    type: str

class Tuple(ty.NamedTuple):
    type: str
    size: int

class List(ty.NamedTuple):
    type: str
    size: int

class Dict(ty.NamedTuple):
    type: str
    keys: ty.Tuple[ty.Any, ...]

class Other(ty.NamedTuple):
    type: str
    value: ty.Any


# The types below are used by `Node._get_node_descriptor()`
class OpTypeDescriptor(ty.NamedTuple):
    type: str
    name: str

class CallableDescriptor(ty.NamedTuple):
    type: str
    source: str

class PathinsDescriptor(ty.NamedTuple):
    type: str
    paths: ty.Tuple[nodeop.PathIn]

class PathoutsDescriptor(ty.NamedTuple):
    type: str
    paths: ty.Tuple[nodeop.PathIn]


# Define Event as the Union of all event types
Event = ty.Union[
    ValueId,
    Ref,
    OpType,
    DataOp,
    PathOut,
    PathIn,
    Node,
    CTX,
    UNSET,
    Tuple,
    List,
    Dict,
    Other,
    OpTypeDescriptor,
    CallableDescriptor,
    PathinsDescriptor,
    PathoutsDescriptor,
]


_EvtT = ty.TypeVar('_EvtT', bound=Event)
#def _event(cls: ty.Callable[..., _EvtT], *k: ty.Any, **kw: ty.Any) -> _EvtT:
def _event(cls: ty.Type[_EvtT], *k: ty.Any, **kw: ty.Any) -> _EvtT:
    return ty.cast(ty.Callable[..., _EvtT], cls)(cls.__name__, *k, **kw)


def walk(op: nodeop.NodeOp) -> ty.Generator[Event, None, None]:
    refs: ty.Dict[int, int] = {}
    yield _event(OpType, type(op))
    if isinstance(op, nodeop.Data):
        yield _event(DataOp, op)
    else:
        for v in op:
            yield from walk_value(v, refs)


def walk_value(value: ty.Any,
               refs: ty.Dict[int, int],
               ) -> ty.Generator[Event, None, None]:
    if id(value) in refs:
        yield _event(Ref, refs[id(value)])
        return

    refs[id(value)] = len(refs)
    yield _event(ValueId, refs[id(value)])

    if isinstance(value, nodeop.PathOut):
        yield _event(PathOut, value)
    elif isinstance(value, nodeop.PathIn):
        yield _event(PathIn, value)
    elif isinstance(value, gn.Node):
        yield _event(Node, value)
    elif value is nodeop.CTX:
        yield _event(CTX)
    elif value is nodeop.UNSET:
        yield _event(UNSET)
    elif type(value) == list:
        yield _event(List, size=len(value))
        for v in value:
            yield from walk_value(v, refs)
    elif type(value) == tuple:
        yield _event(Tuple, size=len(value))
        for v in value:
            yield from walk_value(v, refs)
    elif type(value) == dict:
        keys = tuple(value)
        yield _event(Dict, keys=keys)
        for k in keys:
            yield from walk_value(value[k], refs)
    else:
        yield _event(Other, value)


# NOTE:
# The correct definition for the type below would be::
#
#   NodeDescriptor = ty.Tuple[ty.Union[Event, 'NodeDescriptor'], ...]
#
# However, mypy does not support that yet:
# https://github.com/python/mypy/issues/731
# TODO: use the correct definition when that is fixed.
NodeDescriptor = ty.Tuple[ty.Union[Event, ty.Any], ...]


def node_descriptor(node: gn.Node[ty.Any],
                    cache: ty.Optional[ty.Dict[gn.Node[ty.Any],
                                               NodeDescriptor]] = None,
                    ) -> NodeDescriptor:
    if (cache is not None
            and not isinstance(node._op, nodeop.Value)
            and node in cache):
        return cache[node]

    op = node._op

    if isinstance(op, nodeop.Data):
        # For Data operations, the attribute "id" of the Data operation
        # identifies the data if present. In that case, we remove the
        # payload to make things light and fast.
        if op.id:
            op = op._replace(payload=None)

    desc: ty.List[ty.Union[Event, NodeDescriptor]] = []

    desc.append(_event(PathinsDescriptor, node._pathins))
    desc.append(_event(PathoutsDescriptor, node._pathouts))
    for evt in walk(op):
        if isinstance(evt, Node):
            assert isinstance(evt.value, gn.Node)
            n = evt.value
            evt = evt._replace(value=None)
            desc.append(evt)
            desc.append(node_descriptor(n, cache))
        elif (isinstance(evt, Other)
              and callable(evt.value)
              and not inspect.isbuiltin(evt.value)):
            fn = evt.value
            desc.append(_event(
                CallableDescriptor,
                # NOTE: It would be nice if we could add information from the
                # function's closure and default arguments as well. An issue
                # with that is that it will be common for some unpickable
                # objects to appear.
                source=inspect.getsource(fn),
            ))
        else:
            desc.append(evt)
    desc_tuple: NodeDescriptor = tuple(desc)
    if cache is not None and not isinstance(node._op, nodeop.Value):
        cache[node] = desc_tuple
    return desc_tuple


def resolve_op(op: nodeop.NodeOp,
               ctx: ty.Optional[grun.NodeContext[ty.Any]],
               custom_atom_resolver: ty.Optional[ty.Callable[[Event],
                                                             ty.Any]] = None,
               ) -> nodeop.NodeOp:
    return OpResolver(op, ctx, custom_atom_resolver).resolve()


class _UNRESOLVED:
    __slots__: ty.List[str] = []

    singleton: ty.Optional[_UNRESOLVED] = None

    def __new__(cls) -> _UNRESOLVED:
        if _UNRESOLVED.singleton:
            return _UNRESOLVED.singleton
        r = ty.cast(_UNRESOLVED, super().__new__(cls))
        _UNRESOLVED.singleton = r
        return r


UNRESOLVED = _UNRESOLVED()
"""
Special value to be returned by custom atom resolvers
(`OpResolver.custom_atom_resolver`) when default behavior is expected.
"""


class OpResolver:
    def __init__(self,
                 op: nodeop.NodeOp,
                 ctx: ty.Optional[grun.NodeContext[ty.Any]],
                 custom_atom_resolver: ty.Optional[ty.Callable[[Event],
                                                               ty.Any]] = None,
                 ):
        self.__ctx = ctx
        self.__op = op
        self.__cache: ty.Dict[int, ty.Any] = {}
        self.custom_atom_resolver = custom_atom_resolver

    def resolve(self) -> nodeop.NodeOp:
        if isinstance(self.__op, nodeop.Data):
            return self.__op
        self.__events = walk(self.__op)
        return self.__resolve_op()

    def __resolve_op(self) -> nodeop.NodeOp:
        e = next(self.__events)
        assert isinstance(e, OpType)
        op_type = e.value
        num_args = len(op_type._fields)
        args = [None] * num_args
        for i in range(num_args):
            args[i] = self.__resolve_value()
        constructor = ty.cast(ty.Callable[..., nodeop.NodeOp], op_type)
        return constructor(*args)

    def __resolve_value(self) -> ty.Any:
        evt = next(self.__events)
        if isinstance(evt, Ref):
            return self.__cache[evt.id]

        # Otherwise, evt will be a ValueId
        assert isinstance(evt, ValueId)
        value_id = evt.id

        resolved: ty.Any

        # Now get the next event, which describes the value
        evt = next(self.__events)
        if isinstance(evt, (List, Tuple)):
            resolved = [None] * evt.size
            for i in range(evt.size):
                resolved[i] = self.__resolve_value()
            if isinstance(evt, Tuple):
                resolved = tuple(resolved)
        elif isinstance(evt, Dict):
            keys = evt.keys
            resolved = {}
            for k in keys:
                resolved[k] = self.__resolve_value()
        else:
            # XXX: this cast() call should not be necessary, however mypy is
            # giving a strange error, saying that ``evt`` has incompatible
            # type. It says that the actual type of ``evt`` is a union (listing
            # ``Other`` repeated times in the union list) and that the
            # ``Event`` union was expected.
            resolved = self.__resolve_atom(ty.cast(Event, evt))

        self.__cache[value_id] = resolved
        return resolved

    def __resolve_atom(self, evt: Event) -> ty.Any:
        if self.custom_atom_resolver:
            resolved = self.custom_atom_resolver(evt)
            if resolved is not UNRESOLVED:
                return resolved

        if isinstance(evt, (PathOut, PathIn)):
            resolved = pathlib.Path(evt.value)
        elif isinstance(evt, Node):
            assert evt.value is not None
            resolved = evt.value._result()
        elif isinstance(evt, CTX):
            resolved = self.__ctx
        elif isinstance(evt, UNSET):
            resolved = None
        elif isinstance(evt, Other):
            resolved = evt.value
        else:
            raise RuntimeError(f'unhandled value event, this is probably a bug: {evt!r}')
        return resolved
