# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations

import argparse
import importlib.abc
import importlib.machinery
import importlib.util
import pathlib
import sys

import argparse_subdec

from . import (
    gn,
    grun,
    ty,
    util,
)


class CLI:
    SD = argparse_subdec.SubDec(name_prefix='__cmd_', fn_dest='core_fn')

    def __init__(self, graph: ty.Optional[gn.Graph] = None):
        if graph is None:
            graph = gn._global_graph
        self.__graph = graph
        self.__runner = grun.Runner()
        self.__init_parser()

    def run(self, argv: ty.Optional[ty.Sequence[str]] = None) -> int:
        self.__args = self.__parse_args(argv)
        self.__load_yp()

        if not self.__args.core_subcommand:
            self.__args.core_subcommand = 'run'
            self.__args.core_fn = CLI.__cmd_run

        self.__args.core_fn(self)
        return 0

    def set_graph(self, graph: gn.Graph) -> None:
        self.__graph = graph

    def set_runner(self, runner: grun.Runner) -> None:
        self.__runner = runner

    def __init_parser(self) -> None:
        self.__parser = argparse.ArgumentParser(description='Run Yape!')

        self.__parser.add_argument(
            '--yp',
            default='yp',
            dest='core_yp',
            metavar='YPMODULE',
            help="""Path to entrypoint module or file."""
        )

        self.__subparsers = self.__parser.add_subparsers(
            title='subcommands',
            dest='core_subcommand',
        )

        CLI.SD.create_parsers(self.__subparsers)

    def __parse_args(self,
                     argv: ty.Optional[ty.Sequence[str]],
                     ) -> argparse.Namespace:
        if argv is None:
            argv = sys.argv[1:]
        argv = list(argv)
        return self.__parser.parse_args(argv)

    def __load_yp(self) -> None:
        name = self.__args.core_yp
        sys.path = ['.'] + sys.path
        try:
            spec: ty.Optional[importlib.machinery.ModuleSpec]
            spec = importlib.util.find_spec(name)
            if not spec:
                spec = importlib.util.spec_from_file_location('yp', name)

            if not spec:
                raise RuntimeError(f'failed to find module or file: {name}')

            module = importlib.util.module_from_spec(spec)
            loader = ty.cast(importlib.abc.Loader, spec.loader)
            loader.exec_module(module)
            if hasattr(module, 'nodegen'):
                module.nodegen()
        finally:
            if sys.path and sys.path[0] == '.':
                sys.path = sys.path[1:]

    # SUBCOMMANDS
    # ===========
    # The rest of this class contains definitions of subcommands.

    @SD.cmd(
        description="""
        Run the execution graph. All nodes are used as targets by default. You
        can pass specific targets as positional arguments.
        """
    )
    @SD.add_argument(
        'run_targets',
        default=None,
        nargs='*',
        metavar='TARGET',
    )
    @SD.add_argument(
        '-f', '--force',
        action='store_true',
        dest='run_force',
        help="""
        Run nodes even if cached results are up to date.
        """
    )
    def __cmd_run(self) -> None:
        targets = getattr(self.__args, 'run_targets', None)
        if not targets:
            targets = None

        self.__runner.run(
            graph=self.__graph,
            targets=targets,
            force=getattr(self.__args, 'run_force', False),
            return_results=False,
        )

    @SD.cmd(
        description="""
        List available targets. Only explicitly named targets are listed by
        default. You can use --all to list all targets.
        """
    )
    @SD.add_argument(
        '-a', '--all',
        action='store_true',
        dest='list_all',
        help="""
        List both named and unnamed targets.
        """
    )
    def __cmd_list(self) -> None:
        pred: ty.Optional[ty.Callable[[gn.Node[ty.Any]], bool]] = None
        if not self.__args.list_all:
            def pred(node: gn.Node[ty.Any]) -> bool:
                return node._has_explicit_name
        for node in self.__graph.recurse_nodes(pred):
            print(node._fullname())

    @SD.cmd(
        description="""
        List dependencies for each node passed as target. All nodes are used as
        targets by default. You can pass specific targets as positional
        arguments.
        """,
    )
    @SD.add_argument(
        'deps_targets',
        default=None,
        nargs='*',
        metavar='TARGET',
    )
    def __cmd_deps(self) -> None:
        targets = getattr(self.__args, 'deps_targets', None)
        if not targets:
            targets = None
        nodes, _ = util.parse_targets(targets, self.__graph)
        for node in nodes:
            print(node._fullname())
            for dep in node._get_dep_nodes():
                print(f'    {dep._fullname()}')
