# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations

import pathlib
import subprocess

from . import (
    climodule,
    gn,
    grun,
    mingraphmod,
    nodeop,
    nodestate,
    ty,
)


T = ty.TypeVar('T')


PathIn = nodeop.PathIn


PathOut = nodeop.PathOut


CTX = nodeop.CTX


UNSET = nodeop.UNSET


State = nodestate.State


CachedState = nodestate.CachedState


StateNamespace = nodestate.StateNamespace


CachedStateDB = nodestate.CachedStateDB


Node = gn.Node


Graph = gn.Graph


def graph(**kw) -> gn.Graph:
    return gn.Graph(**kw)


mingraph = mingraphmod.mingraph


def load(path: ty.Union[pathlib.Path, str]) -> gn.Graph:
    """
    Shorthand for ``yape.gn.Graph.load(path)``.
    """
    return gn.Graph.load(path)


# TODO: use ParamSpec instead of ... for callables once that is supported in
# pytype.
@ty.overload
def gr(nodegen: ty.Callable,
       /,
       args: None = None,
       kwargs: None = None,
       **kw) -> ty.Callable[..., gn.Graph]:
    ...
@ty.overload
def gr(nodegen: ty.Callable,
       /,
       args: ty.Sequence,
       kwargs: None,
       **kw,
       ) -> gn.Graph:
    ...
@ty.overload
def gr(nodegen: ty.Callable,
       /,
       args: ty.Sequence,
       kwargs: ty.Mapping,
       **kw,
       ) -> gn.Graph:
    ...
def gr(nodegen, /, args=None, kwargs=None, **kw):
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


@ty.overload
def fn(f: ty.Callable[..., T],
       /,
       args: None = None,
       kwargs: None = None,
       **kw) -> ty.Callable[..., gn.Node[T]]:
    ...
@ty.overload
def fn(f: ty.Callable[..., T],
       /,
       args: ty.Sequence,
       **kw,
       ) -> gn.Node[T]:
    ...
@ty.overload
def fn(f: ty.Callable[..., T],
       /,
       args: ty.Sequence,
       kwargs: ty.Mapping,
       **kw,
       ) -> gn.Node[T]:
    ...
def fn(f, /, args=None, kwargs=None, **kw):
    def node_creator(*call_args, **call_kwargs) -> gn.Node[T]:
        op = nodeop.Call(f, call_args, call_kwargs)
        return gn.Node(op, **kw)

    if args is None and kwargs is None:
        return node_creator

    if args is None:
        args = tuple()

    if kwargs is None:
        kwargs = {}

    return node_creator(*args, **kwargs)


@ty.overload
def value(v: T, /, **kw) -> gn.Node[T]:
    ...
@ty.overload
def value(**kw) -> gn.Node[nodeop._UNSET]:
    ...
def value(v=nodeop.UNSET, /, **kw):
    op = nodeop.Value(v)
    return gn.Node(op, **kw)


def data(payload: T, /, id: str = None, **kw) -> gn.Node[T]:
    op = nodeop.Data(payload, id)
    return gn.Node(op, **kw)


def _cmd_fn(args: ty.Union[str, list, tuple], **subprocess_run_kw):
    if not isinstance(args, str):
        args = tuple(str(arg) for arg in args)
    return subprocess.run(args, **subprocess_run_kw)


def cmd(args: ty.Union[str, list, tuple],
        *,
        node_kw: dict = None,
        **subprocess_run_kw,
        ) -> gn.Node:

    if not isinstance(args, str):
        args = tuple(args)
        name_prefix = args[0] if len(args) else None
    else:
        split = args.split(maxsplit=1)
        name_prefix = split[0] if len(split) else None

    if node_kw is None:
        node_kw = {}

    if 'name_prefix' not in node_kw:
        node_kw['name_prefix'] = name_prefix

    return fn(_cmd_fn, args=[args], kwargs=subprocess_run_kw, **node_kw)


def run(*k, **kw):
    runner = grun.Runner()
    return runner.run(*k, **kw)


cli = climodule.CLI()
