# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations

import collections
import pathlib

from . import (
    gn,
    resmod,
    ty,
)


NODE_T = ty.TypeVar('NODE_T', bound='gn.Node[ty.Any]')


# NodeOp types
# ============
#
# A NodeOp class must be defined as a class that:
#
#   (1) Is decorated with ``_nodeop_cls``.
#   (2) Inherits from ``ty.NamedTuple``.
#   (3) Is present as member of the type union ``NodeOp``.
#
# For example, we can define the event class `A` as follows::
#
##   @_nodeop_cls # (1)
##   class A(ty.NamedTuple): # (2)
##       ... # Tuple fields
##
##   ... # Other classes
##
##   NodeOp = ty.Union[..., A, ...] # (3)
#
# Note: using ## above to make mypy happy.

# _NODE_OP_CLS, _nodeop_classes and _nodeop_cls are used as a mechanism to the
# enforce the conditions for nodeop types.
_NODE_OP_CLS = ty.TypeVar('_NODE_OP_CLS', bound=ty.NamedTuple)
_nodeop_classes = []
def _nodeop_cls(cls: _NODE_OP_CLS) -> _NODE_OP_CLS:
    """
    This decorator must be used for every class to be defined as an nodeop
    type.
    """
    _nodeop_classes.append(cls)
    return cls


@_nodeop_cls
class Data(ty.NamedTuple):
    payload: ty.Any
    id: ty.Optional[str]


@_nodeop_cls
class Value(ty.NamedTuple):
    value: ty.Any


@_nodeop_cls
class GetItem(ty.NamedTuple):
    obj: ty.Any
    key: ty.Any


@_nodeop_cls
class GetAttr(ty.NamedTuple):
    obj: ty.Any
    name: str


@_nodeop_cls
class Call(ty.NamedTuple):
    fn: ty.Callable[..., ty.Any]
    args: ty.Sequence[ty.Any]
    kwargs: ty.Mapping[str, ty.Any]


@_nodeop_cls
class Resource(ty.NamedTuple):
    request: resmod.ResourceRequest[ty.Any]
    handle: ty.Optional[ty.Any]


NodeOp = ty.Union[Data, Value, GetItem, GetAttr, Call, Resource]
# Let's make sure NodeOp union covers all of them
assert set(_nodeop_classes) == set(ty.get_args(NodeOp))


class PathIn(pathlib.PurePosixPath):
    pass


class PathOut(pathlib.PurePosixPath):
    pass


class ResourceIn(ty.Generic[NODE_T]):
    def __init__(self, node: NODE_T):
        if not isinstance(node._op, Resource):
            msg = f'invalid node type: node._op type must be a Resource op'
            raise TypeError(msg)
        self.node = node


class ResourceOut(ty.Generic[NODE_T]):
    def __init__(self, node: NODE_T):
        if not isinstance(node._op, Resource):
            msg = f'invalid node type: node._op type must be a Resource op'
            raise TypeError(msg)
        self.node = node


class _CTX:
    __slots__: ty.List[str] = []

    singleton = None

    def __new__(cls) -> _CTX:
        if _CTX.singleton:
            return _CTX.singleton
        r: _CTX = super().__new__(cls)
        _CTX.singleton = r
        return r

CTX = _CTX()


class _UNSET:
    __slots__: ty.List[str] = []

    singleton = None

    def __new__(cls) -> _UNSET:
        if _UNSET.singleton:
            return _UNSET.singleton
        r: _UNSET = super().__new__(cls)
        _UNSET.singleton = r
        return r


UNSET = _UNSET()


def run_op(op: NodeOp) -> ty.Any:
    if isinstance(op, Data):
        return op.payload
    elif isinstance(op, Value):
        return op.value
    elif isinstance(op, GetItem):
        return op.obj[op.key]
    elif isinstance(op, GetAttr):
        return getattr(op.obj, op.name)
    elif isinstance(op, Call):
        return op.fn(*op.args, **op.kwargs)
    elif isinstance(op, Resource):
        return resmod.get_provider(op.request).create(op.request)
    else:
        raise RuntimeError('unhandled operation, this is probably a bug')
