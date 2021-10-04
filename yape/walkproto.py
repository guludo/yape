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


_event_types = []

def event_type(name: str, *fields: tuple[str]) -> type[collections.namedtuple]:
    """
    A factory function for creating types.

    The resulting type is a namedtuple with "type" as the first field name and
    `fields` as the remaining field names. When creating a new instance of the
    named tuple, the value of the first field will automatically be set to
    `name`.

    The rationale behind such a behavior is that, since
    `Node._get_node_descriptor()` is intended to be used as a way of uniquely
    identifying the node, it is important emit the type of the event together
    with its arguments.
    """
    fields = ('type', *fields)
    base = collections.namedtuple(name, fields)

    def __new__(cls, *k, **kw):
        k = (name, *k)
        return base.__new__(cls, *k, **kw)

    def __getnewargs__(self):
        return self[1:]

    namespace = {
        '__new__': __new__,
        '__getnewargs__': __getnewargs__,
    }

    t = type(name, (base,), namespace)
    _event_types.append(t)
    return t


# The types below are used by `Node._op_walk()` and
# `Node._get_node_descriptor()`
ValueId = event_type('ValueId', 'id')
Ref = event_type('Ref', 'id')
OpType = event_type('OpType', 'value')
DataOp = event_type('DataOp', 'value')
PathOut = event_type('PathOut', 'value')
PathIn = event_type('PathIn', 'value')
Node = event_type('Node', 'value')
CTX = event_type('CTX')
UNSET = event_type('UNSET')
Tuple = event_type('Tuple', 'size')
List = event_type('List', 'size')
Dict = event_type('Dict', 'keys')
Other = event_type('Other', 'value')


# The types below are used by `Node._get_node_descriptor()`
OpTypeDescriptor = event_type('OpTypeDescriptor', 'name')
CallableDescriptor = event_type('CallableDescriptor', 'source')
PathinsDescriptor = event_type('PathinsDescriptor', 'paths')
PathoutsDescriptor = event_type('PathoutsDescriptor', 'paths')


# Define Event as the Union of all event types
Event = ty.Union[tuple(_event_types)]


def walk(op: nodeop.NodeOp):
    refs = {}
    yield OpType(type(op))
    if isinstance(op, nodeop.Data):
        yield DataOp(op)
    else:
        for v in op:
            yield from walk_value(v, refs)


def walk_value(value: ty.Any, refs: dict) -> ty.Generator[Event]:
    if id(value) in refs:
        yield Ref(refs[id(value)])
        return

    refs[id(value)] = len(refs)
    yield ValueId(refs[id(value)])

    if isinstance(value, nodeop.PathOut):
        yield PathOut(value)
    elif isinstance(value, nodeop.PathIn):
        yield PathIn(value)
    elif isinstance(value, gn.Node):
        yield Node(value)
    elif value is nodeop.CTX:
        yield CTX()
    elif value is nodeop.UNSET:
        yield UNSET()
    elif type(value) == list:
        yield List(size=len(value))
        for v in value:
            yield from walk_value(v, refs)
    elif type(value) == tuple:
        yield Tuple(size=len(value))
        for v in value:
            yield from walk_value(v, refs)
    elif type(value) == dict:
        keys = tuple(value)
        yield Dict(keys=keys)
        for k in keys:
            yield from walk_value(value[k], refs)
    else:
        yield Other(value)


def node_descriptor(node: gn.Node,
                    cache: dict[gn.Node, tuple[Event]] = None,
                    ) -> tuple[Event]:
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

    desc = []

    desc.append(PathinsDescriptor(node._pathins))
    desc.append(PathoutsDescriptor(node._pathouts))
    for evt in walk(op):
        if isinstance(evt, Node):
            n = evt.value
            evt = evt._replace(value=None)
            desc.append(evt)
            desc.append(node_descriptor(n, cache))
        elif (isinstance(evt, Other)
              and callable(evt.value)
              and not inspect.isbuiltin(evt.value)):
            fn = evt.value
            desc.append(CallableDescriptor(
                # NOTE: It would be nice if we could add information from the
                # function's closure and default arguments as well. An issue
                # with that is that it will be common for some unpickable
                # objects to appear.
                source=inspect.getsource(fn),
            ))
        else:
            desc.append(evt)
    desc = tuple(desc)
    if cache is not None and not isinstance(node._op, nodeop.Value):
        cache[node] = desc
    return desc


def resolve_op(op: nodeop.NodeOp,
               ctx: grun.NodeContext,
               custom_atom_resolver: ty.Callable = None,
               ) -> nodeop.NodeOp:
    return OpResolver(op, ctx, custom_atom_resolver).resolve()


class _UNRESOLVED:
    __slots__ = []

    singleton = None

    def __new__(cls):
        if _UNRESOLVED.singleton:
            return _UNRESOLVED.singleton
        r = super().__new__(cls)
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
                 ctx: grun.NodeContext,
                 custom_atom_resolver: ty.Callable = None,
                 ):
        self.__ctx = ctx
        self.__op = op
        self.__cache = {}
        self.custom_atom_resolver = custom_atom_resolver

    def resolve(self):
        if isinstance(self.__op, nodeop.Data):
            return self.__op
        self.__events = walk(self.__op)
        return self.__resolve_op()

    def __resolve_op(self) -> nodeop.NodeOp:
        op_type = next(self.__events).value
        num_args = len(op_type._fields)
        args = [None] * num_args
        for i in range(num_args):
            args[i] = self.__resolve_value()
        return op_type(*args)

    def __resolve_value(self) -> ty.Any:
        evt = next(self.__events)
        if isinstance(evt, Ref):
            return self.__cache[evt.id]

        # Otherwise, evt will be a ValueId
        value_id = evt.id

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
            resolved = self.__resolve_atom(evt)

        self.__cache[value_id] = resolved
        return resolved

    def __resolve_atom(self, evt):
        if self.custom_atom_resolver:
            resolved = self.custom_atom_resolver(evt)
            if resolved is not UNRESOLVED:
                return resolved

        if isinstance(evt, (PathOut, PathIn)):
            resolved = pathlib.Path(evt.value)
        elif isinstance(evt, Node):
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
