# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations

import collections

from . import (
    gn,
    ty,
)


def topological_sort(target_nodes: ty.Iterable[gn.Node],
                     ) -> tuple[list[gn.Node], collections.Counter]:
    visited = set()
    visiting = set()
    sorted_nodes = []
    path = []
    stack = [(node, None) for node in target_nodes]
    dependant_counts = collections.Counter()
    while stack:
        node, state = stack.pop()

        if state is None:
            if node in visited:
                # This node and dependencies have already been added to the
                # execution list
                continue

            path.append(node)

            if node in visiting:
                # If I am still visiting this node, this means a circular
                # dependency has been found
                path_str = ' <- '.join(str(n) for n in reversed(path))
                raise Exception(f'circular dependency found between nodes: {path_str}')

            # Get dependencies, push back to stack and mark node as
            # visiting
            deps = list(set(node._get_dep_nodes()))

            for dep in deps:
                dependant_counts[dep] += 1

            stack.append((node, deps))
            visiting.add(node)
        else:
            deps = state
            # If there are no more dependencies to be added to the
            # execution list, the node is ready to be added. If not, then
            # push the next node to the stack.
            if not deps:
                sorted_nodes.append(node)
                visited.add(node)
                visiting.remove(node)
                path.pop()
            else:
                dep = deps.pop()
                stack.append((node, deps))
                stack.append((dep, None))

    return sorted_nodes, dependant_counts


TargetsSpec = ty.Union[
    'gn.NodeRef',
    ty.Callable,
    ty.Sequence['gn.NodeRef'],
    ty.Mapping[str, 'gn.NodeRef'],
]


ParsedTargetsSpec = ty.Union[
    'gn.Node',
    tuple['gn.Node'],
    dict[str, 'gn.NodeRef'],
]


def parse_targets(targets: TargetsSpec,
                  graph: gn.Graph,
                  ) -> tuple[set[fn.Node], ParsedTargetsSpec]:
    if targets is None:
        if not graph:
            raise ValueError('graph is required when targets is None')
        targets = tuple(graph.recurse_nodes())

    def get_node(ref: gn.NodeRef, graph: ty.Union[gn.Graph, None]) -> gn.Node:
        if isinstance(ref, gn.Node):
            return ref
        else:
            if not graph:
                raise ValueError('graph is required when node reference is not a Node instance')

            node = graph.node(ref)

            if not isinstance(node, gn.Node):
                raise RuntimeError('node for ref {ref!r} not found')

            return node

    if isinstance(targets, ty.Mapping):
        targets = {
            k: get_node(v, graph) for k, v in targets.items()
        }
        nodes = set(targets.values())
    elif isinstance(targets, ty.Sequence) and not isinstance(targets, str):
        targets = tuple(get_node(t, graph) for t in targets)
        nodes = set(targets)
    elif callable(targets) and not isinstance(targets, gn.Node):
        if not graph:
            raise ValueError('graph is required when targets is a callable')
        targets = tuple(n for n in graph.recurse_nodes() if targets(n))
        nodes = set(targets)
    else:
        # The remaining possible type to expect is a NodeRef.
        # NOTE: the variable targets here refers to a single node despite its
        # name.
        targets = get_node(targets, graph)
        nodes = {targets}
    return nodes, targets
