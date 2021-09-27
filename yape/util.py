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
