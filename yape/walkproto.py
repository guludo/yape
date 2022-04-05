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
import types

from . import (
    gn,
    grun,
    nodeop,
    ty,
    util,
)


# Event types
# ===========
#
# An event class must be defined as a class that:
#
#   (1) Is decorated with ``_evt_cls``.
#   (2) Inherits from ``ty.NamedTuple``.
#   (3) Has 'type' as the first field, which must be annotated as a ``str``.
#   (4) Is present as member of the type union ``Event``.
#
# For example, we can define the event class `A` as follows::
#
##   @_evt_cls # (1)
##   class A(ty.NamedTuple): # (2)
##       type: str # (3)
##       ... # Other fields here
##
##   ... # Other classes
##
##   Event = ty.Union[..., A, ...] # (4)
#
# Note: using ## above to make mypy happy.

# _EVT_CLS, _evt_classes and _evt_cls are used as a mechanism to enforce the
# conditions for event types.
_EVT_CLS = ty.TypeVar('_EVT_CLS', bound=ty.NamedTuple)
_evt_classes = []
def _evt_cls(cls: _EVT_CLS) -> _EVT_CLS:
    # Check that the named tuple has 'type' as the first field
    assert cls._fields[0] == 'type'
    _evt_classes.append(cls)
    return cls


# Event types for ``Node._op_walk()`` and ``Node._get_node_descriptor()``
# =======================================================================

@_evt_cls
class ValueId(ty.NamedTuple):
    type: str
    id: int


@_evt_cls
class Ref(ty.NamedTuple):
    type: str
    id: int


@_evt_cls
class OpType(ty.NamedTuple):
    type: str
    value: ty.Type[nodeop.NodeOp]


@_evt_cls
class DataOp(ty.NamedTuple):
    type: str
    value: nodeop.NodeOp


@_evt_cls
class PathOut(ty.NamedTuple):
    type: str
    value: nodeop.PathOut


@_evt_cls
class PathIn(ty.NamedTuple):
    type: str
    value: nodeop.PathIn

@_evt_cls
class ResourceOut(ty.NamedTuple):
    type: str
    value: ty.Optional[nodeop.ResourceOut[ty.Any]]

@_evt_cls
class ResourceIn(ty.NamedTuple):
    type: str
    value: ty.Optional[nodeop.ResourceIn[ty.Any]]

@_evt_cls
class Node(ty.NamedTuple):
    type: str
    value: ty.Union[gn.Node[ty.Any], None]


@_evt_cls
class CTX(ty.NamedTuple):
    type: str


@_evt_cls
class UNSET(ty.NamedTuple):
    type: str


@_evt_cls
class Tuple(ty.NamedTuple):
    type: str
    size: int


@_evt_cls
class List(ty.NamedTuple):
    type: str
    size: int


@_evt_cls
class Dict(ty.NamedTuple):
    type: str
    keys: ty.Tuple[ty.Any, ...]


@_evt_cls
class Func(ty.NamedTuple):
    type: str
    code: types.CodeType


@_evt_cls
class Other(ty.NamedTuple):
    type: str
    value: ty.Any


# Event types for ``Node._get_node_descriptor()`` only
# ====================================================

@_evt_cls
class OpTypeDescriptor(ty.NamedTuple):
    type: str
    name: str


@_evt_cls
class PathinsDescriptor(ty.NamedTuple):
    type: str
    paths: ty.Tuple[nodeop.PathIn, ...]


@_evt_cls
class PathoutsDescriptor(ty.NamedTuple):
    type: str
    paths: ty.Tuple[nodeop.PathIn, ...]


@_evt_cls
class ResourceProducersDescriptor(ty.NamedTuple):
    type: str
    # NOTE: use should use ``descriptors: ty.Tuple[NodeDescriptor,...]``, but
    # we would run into cyclic type definitions, which is not currently
    # supported by mypy (https://github.com/python/mypy/issues/731).
    descriptors: ty.Tuple[ty.Any, ...]

@_evt_cls
class ProducedResourceDescriptor(ty.NamedTuple):
    """
    This is used as a marker to reference the node representing the resource to
    be produced.
    """
    type: str


@_evt_cls
class ModuleDescriptor(ty.NamedTuple):
    type: str
    name: str


# Define Event as the Union of all event types
Event = ty.Union[
    ValueId,
    Ref,
    OpType,
    DataOp,
    PathOut,
    PathIn,
    ResourceOut,
    ResourceIn,
    Node,
    CTX,
    UNSET,
    Tuple,
    List,
    Dict,
    Func,
    Other,
    OpTypeDescriptor,
    PathinsDescriptor,
    PathoutsDescriptor,
    ResourceProducersDescriptor,
    ProducedResourceDescriptor,
    ModuleDescriptor,
]
# Let's make sure Event union covers all of them
assert set(_evt_classes) == set(ty.get_args(Event))


_EvtT = ty.TypeVar('_EvtT', bound=Event)
def _event(cls: ty.Type[_EvtT], *k: ty.Any, **kw: ty.Any) -> _EvtT:
    return ty.cast(ty.Callable[..., _EvtT], cls)(cls.__name__, *k, **kw)


# Functions provided by the module
# ================================

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
    elif isinstance(value, nodeop.ResourceOut):
        yield _event(ResourceOut, value)
    elif isinstance(value, nodeop.ResourceIn):
        yield _event(ResourceIn, value)
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
    elif isinstance(value, types.FunctionType):
        c = inspect.getclosurevars(value)
        global_names = tuple(c.globals)
        nonlocals_tuple = tuple(
            c.nonlocals[name] for name in value.__code__.co_freevars
        )
        yield _event(
            Func,
            value.__code__,
        )
        yield from walk_value(c.globals, refs)
        yield from walk_value(nonlocals_tuple, refs)
        yield from walk_value(value.__defaults__, refs)
        yield from walk_value(value.__kwdefaults__, refs)
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
                    *,
                    _resource_node: ty.Optional[gn.Node[ty.Any]] = None,
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
    desc.append(
        _event(
            ResourceProducersDescriptor,
            tuple(
                util.sorted_with_fallback(
                    node_descriptor(n, cache, _resource_node=node)
                    for n in node._resource_producers
                ),
            ),
        ),
    )
    for evt in walk(op):
        if isinstance(evt, Node):
            assert isinstance(evt.value, gn.Node)
            n = evt.value
            evt = evt._replace(value=None)
            desc.append(evt)
            desc.append(node_descriptor(n, cache))
        elif isinstance(evt, (ResourceOut, ResourceIn)):
            assert evt.value is not None
            n = evt.value.node
            desc.append(evt._replace(value=None))
            if (isinstance(evt, ResourceOut)
                    and evt.value.node is _resource_node):
                # When _resource_node is set and is evt.value, that means that
                # it is the resource for wich a ResourceProducersDescriptor is
                # being currently created. Instead of entering into an infinite
                # loop, we use a marker (ProducedResourceDescriptor) to
                # reference the node representing the resource.
                desc.append(_event(ProducedResourceDescriptor))
            else:
                desc.append(node_descriptor(n, cache))
        elif isinstance(evt, Other) \
                and isinstance(evt.value, types.ModuleType):
            desc.append(_event(ModuleDescriptor, evt.value.__name__))
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

        # Now get the next event, which describes the value
        evt = next(self.__events)
        if isinstance(evt, (List, Tuple)):
            resolved = [None] * evt.size
            self.__cache[value_id] = resolved
            for i in range(evt.size):
                resolved[i] = self.__resolve_value()
            if isinstance(evt, Tuple):
                resolved = tuple(resolved)
        elif isinstance(evt, Dict):
            keys = evt.keys
            resolved = {}
            self.__cache[value_id] = resolved
            for k in keys:
                resolved[k] = self.__resolve_value()
        elif isinstance(evt, Func):
            resolved_globals = {}
            resolved_nonlocals = tuple(
                types.CellType()
                for _ in evt.code.co_freevars
            )
            resolved = types.FunctionType(
                evt.code,
                resolved_globals,
                closure=resolved_nonlocals,
            )
            self.__cache[value_id] = resolved

            # Globals
            resolved_globals.update(self.__resolve_value())
            resolved_globals['__builtins__'] = globals()['__builtins__']

            # Nonlocals
            for i, value in enumerate(self.__resolve_value()):
                resolved_nonlocals[i].cell_contents = value

            resolved.__defaults__ = self.__resolve_value()
            resolved.__kwdefaults__ = self.__resolve_value()
        else:
            # XXX: this cast() call should not be necessary, however mypy is
            # giving a strange error, saying that ``evt`` has incompatible
            # type. It says that the actual type of ``evt`` is a union (listing
            # ``Other`` repeated times in the union list) and that the
            # ``Event`` union was expected.
            resolved = self.__resolve_atom(ty.cast(Event, evt))
            self.__cache[value_id] = resolved

        # Make sure we do not forget setting the cache when defining the
        # resolved value. Note that the cache can not simply be updated here
        # because of recursive structures (example: an element of a list point
        # to the list itself).
        assert value_id in self.__cache
        return resolved

    def __resolve_atom(self, evt: Event) -> ty.Any:
        if self.custom_atom_resolver:
            resolved = self.custom_atom_resolver(evt)
            if resolved is not UNRESOLVED:
                return resolved

        if isinstance(evt, (PathOut, PathIn)):
            resolved = pathlib.Path(evt.value)
        elif isinstance(evt, (ResourceOut, ResourceIn)):
            assert evt.value is not None
            resolved = evt.value.node._result()
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
