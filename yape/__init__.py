# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
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


def load(path: ty.Union[pathlib.Path, str]) -> gn.Graph:
    """
    Shorthand for ``yape.gn.Graph.load(path)``.
    """
    return gn.Graph.load(path)


def gr(nodegen: ty.Callable,
       /,
       args=None,
       kwargs=None,
       **kw,
       ) -> ty.Union[ty.Callable, gn.Graph]:
    def graph_creator(*nodegen_args, **nodegen_kwargs) -> gn.Graph:
        g = gn.Graph(**kw)
        with g:
            nodegen(*nodegen_args, **nodegen_kwargs)
        return g

    if args is None and kwargs is None:
        return graph_creator

    if args is None:
        args = tuple()

    if kwargs is None:
        kwargs = {}

    return graph_creator(*args, **kwargs)


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
