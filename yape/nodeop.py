# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations

import collections
import pathlib

from . import ty


class Data(ty.NamedTuple):
    payload: ty.Any
    id: ty.Optional[str]


class Value(ty.NamedTuple):
    value: ty.Any


class GetItem(ty.NamedTuple):
    obj: ty.Any
    key: ty.Any


class GetAttr(ty.NamedTuple):
    obj: ty.Any
    name: str


class Call(ty.NamedTuple):
    fn: ty.Callable[..., ty.Any]
    args: ty.Sequence[ty.Any]
    kwargs: ty.Mapping[str, ty.Any]


NodeOp = ty.Union[Data, Value, GetItem, GetAttr, Call]


class PathIn(pathlib.PurePosixPath):
    pass


class PathOut(pathlib.PurePosixPath):
    pass


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
    else:
        raise RuntimeError('unhandled operation, this is probably a bug')
