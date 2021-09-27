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


CTX = object()


UNSET = object()


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
