# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations

import pathlib
import logging
import pickle
import typing

from . import (
    mingraphmod,
    nodeop,
    nodestate,
    ty,
    util,
    walkproto,
)


logger = logging.getLogger()


NodeName = ty.Union[str, ty.Iterable[str]]


NodeRef = ty.Union[NodeName, 'Node']


T = typing.TypeVar('T')


class Node(typing.Generic[T]):
    def __init__(self,
            op: nodeop.NodeOp,
            name: str = None,
            name_prefix: str = None,
            always: bool = False,
            pathins: ty.Iterable[pathlib.PurePath] = tuple(),
            pathouts: ty.Iterable[pathlib.PurePath] = tuple(),
            parent: Graph = None,
            no_parent: bool = False,
            ):
        if not parent:
            if _graph_build_stack:
                parent = _graph_build_stack[-1]
            elif not no_parent:
                parent = _global_graph

        if name and '/' in name:
            raise ValueError(f'node name can not contain the slash character ("/"): {name}')

        self._op: nodeop.NodeOp = op
        self._name = name
        self._name_prefix = name_prefix
        self._always = always

        pins = set(nodeop.PathIn(p) for p in pathins)
        pouts = set(nodeop.PathOut(p) for p in pathouts)
        for evt in self.__op_walk():
            if isinstance(evt, walkproto.PathOut):
                pouts.add(evt.value)
            elif isinstance(evt, walkproto.PathIn):
                pins.add(evt.value)
        self._pathins: ty.Tuple[nodeop.PathIn, ...] = tuple(sorted(pins))
        self._pathouts: ty.Tuple[nodeop.PathOut, ...] = tuple(sorted(pouts))

        self.__parent = parent

        if self.__parent:
            self.__parent._Graph__add_node(self) # type: ignore[attr-defined]
        else:
            # Validate things if node is outside of a graph
            if self._name:
                raise ValueError('named node is only allowed inside a graph')
            if self._pathouts:
                raise ValueError('pathouts are only allowed inside a graph')
            if self._pathins:
                raise ValueError('pathins are only allowed inside a graph')

    def _fullname(self) -> ty.Optional[str]:
        if not self._name:
            return None

        stack = [self._name]
        g = self.__parent
        if g:
            root: Graph = g._Graph__root # type: ignore[attr-defined]
        while g and g != root:
            if g.name is None:
                raise ValueError('one of the parent graphs has no name')
            stack.append(g.name)
            g = g._Graph__parent # type: ignore[attr-defined]
        return '/'.join(reversed(stack))

    def _set(self, value):
        if not isinstance(self._op, nodeop.Value):
            raise ValueError('a value for a node can be set or unset only for Value operators')
        self._op = nodeop.Value(value)

    def _unset(self):
        self._set(nodeop.UNSET)

    def _result(self) -> T:
        return nodestate.get_state(self).get_result()

    def _must_run(self) -> bool:
        if self._always:
            return True
        return not nodestate.get_state(self).is_up_to_date()

    def __op_walk(self, op: nodeop.NodeOp = None,
                  ) -> ty.Generator[walkproto.Event, None, None]:
        if not op:
            op = self._op
        yield from walkproto.walk(op)

    def _get_dep_nodes(self) -> ty.Generator[Node, None, None]:
        for evt in self.__op_walk():
            if isinstance(evt, walkproto.Node):
                assert evt.value is not None
                yield evt.value
        for p in self._pathins:
            if self.__parent is not None:
                dep = self.__parent.path_producer(pathlib.Path(p))
                if dep:
                    yield dep

    def _get_node_descriptor(self) -> walkproto.NodeDescriptor:
        return walkproto.node_descriptor(self)

    def __getitem__(self, key) -> Node:
        return Node(nodeop.GetItem(self, key))

    def __getattr__(self, name) -> Node:
        if name in ('__getstate__', '__setstate__'):
            raise AttributeError('__setstate__ and __setstate__ are reserved for pickle')
        if isinstance(name, str) and name[0] == '_':
            msg = (
                f'failed to get attribute {name!r}: '
                'attributes starting with "_" are not supported via the dot operator (".")'
            )
            raise ValueError(msg)
        return Node(nodeop.GetAttr(self, name))

    def __call__(self, *args, **kwargs) -> Node:
        return Node(nodeop.Call(self, args, kwargs))

    def __str__(self) -> str:
        return f'<{self._fullname()}>'

    def __repr__(self) -> str:
        return f'Node({self._op})'


class Graph:
    def __init__(self,
                name: str = None,
                parent: Graph = None,
                no_parent: bool = False,
             ):
        if name and '/' in name:
            raise ValueError(f'graph name can not contain the slash character ("/"): {name}')

        if not parent:
            if _graph_build_stack:
                parent = _graph_build_stack[-1]
            elif not no_parent:
                parent = _global_graph

        self.name: ty.Optional[str] = name
        self.__in_build_context = False
        self.__nodes: ty.List[Node] = []
        self.__parent = parent
        self.__root: Graph = parent.__root if parent else self
        self.__graphs: ty.List[Graph] = []

        self.__name2node: ty.Dict[str, ty.Union[Node, Graph]] = {}
        """
        A dictionary mapping strings to either `Node` or `Graph` instances.
        """

        self.__pathout2node: ty.Dict[nodeop.PathOut, Node] = {}
        """
        A dictionary mapping `nodeop.PathOut` instances to nodes that declare
        to produce them. This object should not be consulted directly, use the
        method `path_producer()` instead.
        """

        if self.__parent:
            self.__parent.__add_graph(self)

    def __enter__(self):
        if self.__in_build_context:
            raise RuntimeError('graph already in build context')
        _graph_build_stack.append(self)
        self.__in_build_context = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__in_build_context = False
        _graph_build_stack.pop()

    def fullname(self) -> str:
        stack = []
        g = self
        while g != self.__root:
            if g.name is None:
                msg = 'one of the graphs in the hierarchy has no name'
                raise ValueError(msg)
            stack.append(g.name)
            assert g.__parent is not None
            g = g.__parent
        return '/'.join(reversed(stack))

    def save(self, path: ty.Union[pathlib.Path, str]):
        if self.__in_build_context:
            msg = 'can not save a graph that is currently in build context'
            raise RuntimeError(msg)

        with open(path, 'wb') as f:
            CustomPickler(f).dump(self)

    @staticmethod
    def load(path: ty.Union[pathlib.Path, str]) -> Graph:
        with open(path, 'rb') as f:
            return pickle.load(f)

    def node(self, path: NodeName) -> ty.Union[Node, Graph]:
        if isinstance(path, str):
            parts = tuple(path.split('/'))
        else:
            parts = tuple(path)

        if not parts:
            raise KeyError('received empty path as key')

        graph_names = parts[:-1]
        node_name = parts[-1]

        cur_graph = self
        for i, graph_name in enumerate(graph_names):
            if graph_name not in cur_graph.__name2node:
                partial_path = parts[:i]
                raise KeyError(f'graph at {partial_path!r} does not contain a child named {graph_name!r}')
            next_graph = cur_graph.__name2node[graph_name]
            if not isinstance(next_graph, Graph):
                partial_path = parts[:i+1]
                raise KeyError(f'element at {partial_path!r} is not a graph')
            cur_graph = next_graph

        if node_name not in cur_graph.__name2node:
            raise KeyError(f'graph at {graph_names!r} does not contain a node named {node_name!r}')

        return cur_graph.__name2node[node_name]

    def recurse_nodes(self) -> ty.Generator[Node, None, None]:
        yield from self.__nodes
        for g in self.__graphs:
            yield from g.recurse_nodes()

    def path_producer(self, path: pathlib.Path) -> ty.Optional[Node]:
        """
        Return the node that declares to produce the path `path` or None if there
        is no such node.
        """
        p = nodeop.PathOut(path)
        if p in self.__root.__pathout2node:
            return self.__root.__pathout2node[p]
        return _global_graph.__pathout2node.get(p)

    def mingraph(self,
                 unbounds: util.TargetsSpec,
                 targets: util.TargetsSpec,
                 ) -> Graph:
        return mingraphmod.mingraph(unbounds, targets, graph=self)

    def __add_graph(self, graph: Graph):
        if not graph.name:
            graph.name = f'graph-{len(self.__graphs)}'

        if graph.name in self.__name2node:
            msg = f'there is already a node named "{graph.name}"'
            raise ValueError(msg)
        self.__name2node[graph.name] = graph

        self.__graphs.append(graph)

    def __add_node(self, node: Node):
        if not node._name:
            prefix = node._name_prefix
            if not prefix:
                if (isinstance(node._op, nodeop.Call)
                        and hasattr(node._op.fn, '__name__')):
                    prefix = node._op.fn.__name__
                else:
                    prefix= 'unnamed'
            idx = 0
            node._name = prefix
            while node._name in self.__name2node:
                idx += 1
                node._name = f'{prefix}-{idx}'

        if node._name in self.__name2node:
            msg = f'there is already a node named "{node._name}"'
            raise ValueError(msg)
        self.__name2node[node._name] = node

        for p in node._pathouts:
            if self.path_producer(pathlib.Path(p)):
                msg = f'found multiple nodes declaring to produce {p}'
                raise ValueError(msg)
            self.__root.__pathout2node[p] = node

        self.__nodes.append(node)


class CustomPickler(pickle.Pickler):
    def reducer_override(self, obj):
        if getattr(obj, '__module__', '') == '__main__':
            msg = (
                f'the object {obj} is defined in the __main__ module, '
                'you may run into issues if loading this graph from another module'
            )
            logger.warning(msg)
        return NotImplemented

_graph_build_stack: ty.List[Graph] = []
_global_graph = Graph(no_parent=True)
