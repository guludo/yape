# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""
"""
from __future__ import annotations

import contextlib
import pathlib

from . import (
    gn,
    nodeop,
    nodestate,
    ty,
    util,
    walkproto,
    yapecontext,
)


T = ty.TypeVar('T')


RunResult = ty.Union[
    ty.Any,
    ty.Tuple[ty.Any, ...],
    ty.Dict[str, ty.Any],
]


class Runner:
    def run(self,
            targets: ty.Optional[util.TargetsSpec] = None,
            graph: ty.Optional[gn.Graph] = None,
            context: ty.Optional[yapecontext.YapeContext] = None,
            force: bool = False,
            return_results: bool = True,
            ) -> RunResult:
        target_nodes, targets = util.parse_targets(targets, graph)

        # Get nodes to be executed
        nodes_to_run, dependant_counts = util.topological_sort(target_nodes)

        ctx: ty.Union[yapecontext.YapeContext, ty.ContextManager[None]]
        if context is None:
            if yapecontext._current_context is None:
                ctx = yapecontext.YapeContext()
            else:
                # Let's make this a null context since a context is already in
                # place.
                ctx = contextlib.nullcontext()
        else:
            ctx = context

        return_value: RunResult
        # Run nodes
        with ctx:
            for node in nodes_to_run:
                if not node._must_run():
                    if not (force and node in target_nodes):
                        continue
                node_ctx = NodeContext(node)
                resolved_op = walkproto.resolve_op(node._op, node_ctx)
                for pout in node._pathouts:
                    p = pathlib.Path(pout)
                    if p.parent:
                        p.parent.mkdir(parents=True, exist_ok=True)
                result = nodeop.run_op(resolved_op)
                nodestate.get_state(node).set_result(result)
                for dep in node._get_dep_nodes():
                    dependant_counts[dep] -= 1
                    if not dependant_counts[dep] and dep not in target_nodes:
                        nodestate.get_state(dep).release()

            # Generate return value
            if not return_results:
                return_value = None
            elif isinstance(targets, dict):
                return_value = {
                    k: n._result() for k, n in targets.items()
                }
            elif isinstance(targets, tuple):
                return_value = tuple(n._result() for n in targets)
            else:
                return_value = targets._result()

        return return_value


class NodeContext(ty.Generic[T]):
    def __init__(self, node: gn.Node[T]):
        self.__node = node

    def workdir(self) -> ty.Optional[pathlib.Path]:
        return nodestate.get_state(self.__node).workdir()
