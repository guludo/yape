# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""
"""
from __future__ import annotations

import contextlib

from . import (
    gn,
    nodeop,
    nodestate,
    ty,
    util,
    walkproto,
)


RunResult = ty.Union[
    'ty.Any',
    tuple,
    dict[str, 'ty.Any'],
]


class Runner:
    def run(self,
            targets: util.TargetsSpec = None,
            graph: gn.Graph = None,
            ns: nodestate.StateNamespace = None,
            ) -> RunResult:
        target_nodes, targets = util.parse_targets(targets, graph)

        # Get nodes to be executed
        nodes_to_run, dependant_counts = util.topological_sort(target_nodes)

        # Get or create the state namespace
        node_state_ctx = contextlib.nullcontext()
        if not nodestate._current_namespace:
            if not ns:
                ns = nodestate.StateNamespace()
            node_state_ctx = ns

        # Run nodes
        with node_state_ctx:
            for node in nodes_to_run:
                if not node._must_run():
                    continue
                ctx = NodeContext(node)
                resolved_op = walkproto.resolve_op(node._op, ctx)
                result = nodeop.run_op(resolved_op)
                nodestate.get_state(node).set_result(result)
                for dep in node._get_dep_nodes():
                    dependant_counts[dep] -= 1
                    if not dependant_counts[dep] and dep not in target_nodes:
                        nodestate.get_state(dep).release()

            # Generate return value
            if isinstance(targets, dict):
                return_value = {
                    k: n._result() for k, n in targets.items()
                }
            elif isinstance(targets, tuple):
                return_value = tuple(n._result() for n in targets)
            else:
                return_value = targets._result()

        return return_value


class NodeContext:
    def __init__(self, node: gn.Node):
        self.__node = node

    def workdir(self):
        return nodestate.get_state(self.__node).workdir()
