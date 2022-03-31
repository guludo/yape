# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations

import inspect
import pathlib
import subprocess

from . import (
    climodule,
    gn,
    grun,
    mingraphmod,
    nodeop,
    nodestate,
    pathprovider,
    resmod,
    ty,
    yapecontext,
)


T = ty.TypeVar('T')


PathIn = nodeop.PathIn
PathOut = nodeop.PathOut
ResourceIn = nodeop.ResourceIn
ResourceOut = nodeop.ResourceOut
CTX = nodeop.CTX
UNSET = nodeop.UNSET


State = nodestate.State
CachedState = nodestate.CachedState
StateNamespace = nodestate.StateNamespace
CachedStateDB = nodestate.CachedStateDB


ResourceRequest = resmod.ResourceRequest
ResourceProvider = resmod.ResourceProvider


PathRequest = pathprovider.PathRequest
PathProvider = pathprovider.PathProvider


Node = gn.Node
Graph = gn.Graph


YapeContext = yapecontext.YapeContext


mingraph = mingraphmod.mingraph


def graph(**kw: ty.Any) -> gn.Graph:
    return gn.Graph(**kw)


def load(path: ty.Union[pathlib.Path, str]) -> gn.Graph:
    """
    Shorthand for ``yape.gn.Graph.load(path)``.
    """
    return gn.Graph.load(path)


@ty.overload
def input(v: ty.Union[pathlib.PurePath, str],
          *rest: ty.Union[pathlib.PurePath, str],
          ) -> nodeop.PathIn:
    ...
@ty.overload
def input(v: gn.Node[T]) -> nodeop.ResourceIn[gn.Node[T]]:
    ...
def input(v: ty.Union[pathlib.PurePath, str, gn.Node[T]],
          *rest: ty.Union[pathlib.PurePath, str],
          )-> ty.Union[nodeop.PathIn, nodeop.ResourceIn[gn.Node[T]]]:
    if isinstance(v, (pathlib.PurePath, str)):
        return nodeop.PathIn(v, *rest)
    elif isinstance(v, gn.Node):
        if rest:
            raise ValueError('variadic arguments are allowed only for paths')
        if not isinstance(v._op, nodeop.Resource):
            raise ValueError('expected operator to be Resource')
        return nodeop.ResourceIn(v)
    else:
        raise TypeError('invalid type for v')


@ty.overload
def output(v: ty.Union[pathlib.PurePath, str],
          *rest: ty.Union[pathlib.PurePath, str],
           ) -> nodeop.PathOut:
    ...
@ty.overload
def output(v: gn.Node[T]) -> nodeop.ResourceOut[gn.Node[T]]:
    ...
def output(v: ty.Union[pathlib.PurePath, str, gn.Node[T]],
          *rest: ty.Union[pathlib.PurePath, str],
           ) -> ty.Union[nodeop.PathOut, nodeop.ResourceOut[gn.Node[T]]]:
    if isinstance(v, (pathlib.PurePath, str)):
        return nodeop.PathOut(v, *rest)
    elif isinstance(v, gn.Node):
        if rest:
            raise ValueError('variadic arguments are allowed only for paths')
        if not isinstance(v._op, nodeop.Resource):
            raise ValueError('expected operator to be Resource')
        return nodeop.ResourceOut(v)
    else:
        raise TypeError('invalid type for v')


# TODO: use ParamSpec instead of ... for callables once that is supported in
# mypy.
@ty.overload
def gr(nodegen: ty.Callable[..., ty.Any],
       /,
       args: None = None,
       kwargs: None = None,
       **kw: ty.Any,
       ) -> ty.Callable[..., gn.Graph]:
    ...
@ty.overload
def gr(nodegen: ty.Callable[..., ty.Any],
       /,
       args: ty.Sequence[ty.Any],
       kwargs: None = None,
       **kw: ty.Any,
       ) -> gn.Graph:
    ...
@ty.overload
def gr(nodegen: ty.Callable[..., ty.Any],
       /,
       args: ty.Sequence[ty.Any],
       kwargs: ty.Mapping[str, ty.Any],
       **kw: ty.Any,
       ) -> gn.Graph:
    ...
def gr(nodegen: ty.Callable[..., ty.Any],
       /,
       args: ty.Optional[ty.Sequence[ty.Any]] = None,
       kwargs: ty.Optional[ty.Mapping[str, ty.Any]] = None,
       **kw: ty.Any,
       ) -> ty.Union[ty.Callable[..., gn.Graph], gn.Graph]:
    def graph_creator(*nodegen_args: ty.Any, **nodegen_kwargs: ty.Any) -> gn.Graph:
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
       **kw: ty.Any,
       ) -> ty.Callable[..., gn.Node[T]]:
    ...
@ty.overload
def fn(f: ty.Callable[..., T],
       /,
       args: ty.Sequence[ty.Any],
       **kw: ty.Any,
       ) -> gn.Node[T]:
    ...
@ty.overload
def fn(f: ty.Callable[..., T],
       /,
       args: ty.Sequence[ty.Any],
       kwargs: ty.Mapping[str, ty.Any],
       **kw: ty.Any,
       ) -> gn.Node[T]:
    ...
def fn(f: ty.Callable[..., T],
       /,
       args: ty.Optional[ty.Sequence[ty.Any]] = None,
       kwargs: ty.Optional[ty.Mapping[str, ty.Any]] = None,
       **kw: ty.Any,
       ) -> ty.Union[ty.Callable[..., gn.Node[T]], gn.Node[T]]:
    def node_creator(*call_args: ty.Any, **call_kwargs: ty.Any) -> gn.Node[T]:
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
def value(v: T, /, **kw: ty.Any) -> gn.Node[T]:
    ...
@ty.overload
def value(**kw: ty.Any) -> gn.Node[nodeop._UNSET]:
    ...
def value(v: ty.Any = nodeop.UNSET, /, **kw: ty.Any) -> gn.Node[ty.Any]:
    op = nodeop.Value(v)
    return gn.Node(op, **kw)


def data(payload: T,
         /,
         id: ty.Optional[str] = None,
         **kw: ty.Any,
         ) -> gn.Node[T]:
    op = nodeop.Data(payload, id)
    return gn.Node(op, **kw)


@ty.overload
def res(request: None = None, /, **kw: ty.Any) -> gn.Node[pathlib.Path]:
    ...
@ty.overload
def res(request: resmod.ResourceRequest[T], /, **kw: ty.Any) -> gn.Node[T]:
    ...
def res(request: ty.Optional[resmod.ResourceRequest[T]] = None,
        /,
        **kw: ty.Any,
        ) -> ty.Union[gn.Node[pathlib.Path], gn.Node[T]]:
    if request is None:
        op = nodeop.Resource(pathprovider.PathRequest(), None)
        return ty.cast(gn.Node[pathlib.Path], gn.Node(op, **kw))
    else:
        op = nodeop.Resource(request, None)
        return ty.cast(gn.Node[T], gn.Node(op, **kw))


def _cmd_fn(args: ty.Union[str, ty.List[ty.Any], ty.Tuple[ty.Any, ...]],
            **subprocess_run_kw: ty.Any,
            ) -> subprocess.CompletedProcess[ty.Any]:
    if not isinstance(args, str):
        args = tuple(str(arg) for arg in args)
    return subprocess.run(args, **subprocess_run_kw)


def cmd(args: ty.Union[str, ty.List[ty.Any], ty.Tuple[ty.Any, ...]],
        *,
        node_kw: ty.Optional[ty.Dict[str, ty.Any]] = None,
        **subprocess_run_kw: ty.Any,
        ) -> gn.Node[subprocess.CompletedProcess[ty.Any]]:
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


@ty.overload
def node(f: ty.Callable[..., T],
         /,
         **kw: ty.Any,
         ) -> gn.Node[T]:
    ...
@ty.overload
def node(f: None = None,
         /,
         **kw: ty.Any,
         ) -> ty.Callable[[ty.Callable[..., T]], gn.Node[T]]:
    ...
def node(f: ty.Optional[ty.Callable[..., T]] = None,
         /,
         **kw: ty.Any,
         ) -> ty.Union[gn.Node[T],
                       ty.Callable[[ty.Callable[..., T]], gn.Node[T]],
                       ]:
    def decorator(f: ty.Callable[..., T]) -> gn.Node[T]:
        sig = inspect.signature(f)
        bound = sig.bind()
        bound.apply_defaults()
        return fn(f, bound.args, bound.kwargs, **kw)

    if f is None:
        return decorator
    return decorator(f)


def run(*k: ty.Any, **kw: ty.Any) -> grun.RunResult:
    runner = grun.Runner()
    return runner.run(*k, **kw)


cli = climodule.CLI()
