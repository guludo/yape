from __future__ import annotations

from . import (
    gn,
    grun,
    mingraph,
    nodeop,
    nodestate,
    ty,
)


PathIn = nodeop.PathIn


PathOut = nodeop.PathOut


CTX = nodeop.CTX


UNSET = nodeop.UNSET


State = nodestate.State


CachedState = nodestate.CachedState


StateNamespace = nodestate.StateNamespace


CachedStateDB = nodestate.CachedStateDB



def graph(**kw) -> gn.Graph:
    return gn.Graph(**kw)


mingraph = mingraph.mingraph


def fn(f: ty.Any,
       /,
       args=None,
       kwargs=None,
       **kw,
       ) -> ty.Union[ty.Callable, gn.Node]:
    def node_creator(*call_args, **call_kwargs) -> gn.Node:
        op = nodeop.Call(f, call_args, call_kwargs)
        return gn.Node(op, **kw)

    if args is None and kwargs is None:
        return node_creator

    if args is None:
        args = tuple()

    if kwargs is None:
        kwargs = {}

    return node_creator(*args, **kwargs)


def value(v: ty.Any = nodeop.UNSET, /, **kw) -> gn.Node:
    op = nodeop.Value(v)
    return gn.Node(op, **kw)


def data(payload: ty.Any, /, id: str = None, **kw) -> gn.Node:
    op = nodeop.Data(payload, id)
    return gn.Node(op, **kw)


def run(*k, **kw):
    runner = grun.Runner()
    return runner.run(*k, **kw)
