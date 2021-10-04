# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations

import collections
import pathlib

from . import ty


_op_types = []
def op_namedtuple(type_name, *names, **kw):
    t = collections.namedtuple(type_name, names, **kw)
    _op_types.append(t)
    return t


Data = op_namedtuple('Data', 'payload', 'id')


Value = op_namedtuple('Value', 'value')


GetItem = op_namedtuple('GetItem', 'obj', 'key')


GetAttr = op_namedtuple('GetAttr', 'obj', 'name')


Call = op_namedtuple('Call', 'fn', 'args', 'kwargs')


class PathIn(pathlib.PurePosixPath):
    pass


class PathOut(pathlib.PurePosixPath):
    pass


class _CTX:
    __slots__ = []

    singleton = None

    def __new__(cls):
        if _CTX.singleton:
            return _CTX.singleton
        r = super().__new__(cls)
        _CTX.singleton = r
        return r

CTX = _CTX()


class _UNSET:
    __slots__ = []

    singleton = None

    def __new__(cls):
        if _UNSET.singleton:
            return _UNSET.singleton
        r = super().__new__(cls)
        _UNSET.singleton = r
        return r


UNSET = _UNSET()


NodeOp = ty.Union[tuple(_op_types)]


def run_op(op):
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
