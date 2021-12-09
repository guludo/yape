# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations

from . import (
    gn,
    nodeop,
    walkproto,
    ty,
    util,
)


T = ty.TypeVar('T')


def mingraph(unbounds: util.TargetsSpec,
             targets: util.TargetsSpec,
             graph: ty.Optional[gn.Graph] = None,
             dest: ty.Optional[gn.Graph] = None,
             ) -> gn.Graph:
    return MingraphBuilder(unbounds, targets, graph, dest).build()


class MingraphBuilder:
    def __init__(self,
                 unbounds: util.TargetsSpec,
                 targets: util.TargetsSpec,
                 graph: ty.Optional[gn.Graph] = None,
                 dest: ty.Optional[gn.Graph] = None,
                 ):
        to_be_unbound, unbounds = util.parse_targets(unbounds, graph)
        target_nodes, targets = util.parse_targets(targets, graph)

        self.__to_be_unbound = to_be_unbound
        self.__unbounds = unbounds
        self.__target_nodes = target_nodes
        self.__targets = targets
        self.__dest = dest or gn.Graph()
        self.__new_nodes_cache: ty.Dict[gn.Node[ty.Any], gn.Node[ty.Any]] = {}

        # Dictionary that will tell whether a node transitively depends on a
        # node to be unbound. This is used as cache for the method
        # __depends_on_unbound()
        self.__depends_on_unbound_cache: ty.Dict[gn.Node[ty.Any], bool] = {}

        self.__generate_future_names()

    def build(self) -> gn.Graph:
        with self.__dest:
            for node in self.__target_nodes:
                self.__get_new_node(node)
        return self.__dest

    def __get_new_node(self,
                       node: gn.Node[T],
                       ) -> gn.Node[ty.Union[T, nodeop._UNSET]]:
        if node in self.__new_nodes_cache:
            return self.__new_nodes_cache[node]

        if self.__depends_on_unbound(node) and node in self.__to_be_unbound:
            raise RuntimeError(
                f'node {node} is unbound and depends on an unbound'
            )
        elif self.__depends_on_unbound(node):
            op = walkproto.resolve_op(
                node._op,
                None,
                self.__custom_atom_resolver,
            )
        elif node in self.__to_be_unbound:
            op = nodeop.Value(nodeop.UNSET)
        else:
            op = nodeop.Data(node._result(), id=None)

        new_node: gn.Node[ty.Union[T, nodeop._UNSET]]
        new_node = gn.Node(op, name=self.__future_names.get(node))
        self.__new_nodes_cache[node] = new_node
        return new_node

    def __depends_on_unbound(self,
                             node: gn.Node[ty.Any],
                             visited: ty.Optional[ty.Set[gn.Node[ty.Any]]] = None,
                             ) -> bool:
        if node in self.__depends_on_unbound_cache:
            return self.__depends_on_unbound_cache[node]

        if visited is None:
            visited = set()

        if node in visited:
            raise RuntimeError('cycle detected')
        visited.add(node)

        r = False
        for dep in node._get_dep_nodes():
            if dep in self.__to_be_unbound:
                r = True
                break

            if self.__depends_on_unbound(dep, visited):
                r =  True
                break

        self.__depends_on_unbound_cache[node] = r
        return r

    def __custom_atom_resolver(self, evt: walkproto.Event) -> ty.Any:
        if isinstance(evt, walkproto.Node):
            assert evt.value is not None
            return self.__get_new_node(evt.value)
        elif isinstance(evt, (walkproto.PathOut, walkproto.PathIn)):
            return evt.value
        elif isinstance(evt, walkproto.CTX):
            return nodeop.CTX
        else:
            return walkproto.UNRESOLVED

    def __generate_future_names(self) -> None:
        self.__future_names: ty.Dict[gn.Node[ty.Any], str] = {}

        if isinstance(self.__unbounds, dict):
            for name, node in self.__unbounds.items():
                if node in self.__future_names:
                    raise ValueError(
                        f'multiple future names for node {node}: '
                        f'{name!r} and {self.__future_names[node]!r}'
                    )
                self.__future_names[node] = name

        if isinstance(self.__targets, dict):
            for name, node in self.__targets.items():
                if isinstance(self.__unbounds, dict) \
                        and name in self.__unbounds:
                    raise ValueError(
                        f'the target node {node} has a future name ({name!r}) '
                        f'already used for an unbound node'
                    )
                if node in self.__future_names:
                    raise ValueError(
                        f'multiple future names for node {node}: '
                        f'{name!r} and {self.__future_names[node]!r}'
                    )
                self.__future_names[node] = name
