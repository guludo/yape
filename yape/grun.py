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


RunTargets = ty.Union[
    'gn.NodeRef',
    ty.Sequence['gn.NodeRef'],
    ty.Mapping[str, 'gn.NodeRef'],
]


RunResult = ty.Union[
    'ty.Any',
    tuple,
    dict[str, 'ty.Any'],
]


class Runner:
    def run(self,
            targets: RunTargets = None,
            graph: gn.Graph = None,
            ns: nodestate.StateNamespace = None,
            ) -> RunResult:
        if targets is None:
            if not graph:
                raise ValueError('graph is required when targets is None')
            targets = tuple(graph.recurse_nodes())

        # Generate set of target nodes
        if isinstance(targets, ty.Mapping):
            targets = {
                k: self.__get_target_node(v, graph) for k, v in targets.items()
            }
            target_nodes = set(targets.values())
        elif isinstance(targets, ty.Sequence) and not isinstance(targets, str):
            targets = tuple(self.__get_target_node(t, graph) for t in targets)
            target_nodes = set(targets)
        else:
            # The remaining possible type to expect is a NodeRef.
            # NOTE: only one target, but we keep using the variable targets
            # (plural) for consistency
            targets = self.__get_target_node(targets, graph)
            target_nodes = {targets}

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
            if isinstance(targets, ty.Mapping):
                return_value = {
                    k: n._result() for k, n in targets.items()
                }
            elif isinstance(targets, tuple):
                return_value = tuple(n._result() for n in targets)
            else:
                return_value = targets._result()

        return return_value

    @staticmethod
    def __get_target_node(ref: gn.NodeRef, graph: ty.Union[gn.Graph, None]):
        if isinstance(ref, gn.Node):
            return ref
        else:
            if not graph:
                raise ValueError('graph is required when node reference is not a Node instance')

            node = graph.node(ref)

            if not isinstance(node, gn.Node):
                raise RuntimeError('node for ref {ref!r} not found')

            return node


class NodeContext:
    def __init__(self, node: gn.Node):
        self.__node = node

    def workdir(self):
        return nodestate.get_state(self.__node).workdir()
