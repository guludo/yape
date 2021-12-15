# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""
This module provides the class ``YapeContext``, which holds context objects
necessary for some operations on nodes (e.g., running them).
"""
from __future__ import annotations
import contextlib
import pathlib
import types

from . import (
    nodestate,
    ty,
)


class YapeContext:
    """
    A ``YapeContext`` is a context manager that holds objects necessary for
    some operations on nodes. It must be used as context manager for some
    operations on nodes, for example, to run and get results from nodes.
    """
    def __init__(self,
                 state_namespace: ty.Optional[nodestate.StateNamespace] = None,
                 use_cached_state: bool = True,
                 state_cache_path: ty.Union[str, pathlib.Path, None] = None,
                 ):
        """
        Initialize the context object.

        All parameters are set as attributes of this object and can be mutated
        at will before the context is used.

        :param state_namespace: a ``StateNamespace`` object to be used as state
          namespace. If omitted, a default one is created and used.

        :param use_cached_state: if true (the default), then the default state
          namespace will use a cache in the file system (``CachedStateDB``) for
          states. This is only applicable if ``state_namespace`` is omitted.

        :param state_cache_path: the path to be used for the state cache. This
          is only applicable if ``use_cached_state`` is a true value. If
          omitted, then ``yape.nodestate.DEFAULT_DB_DIR`` is used.
        """
        self.state_namespace = state_namespace
        self.use_cached_state = use_cached_state
        self.state_cache_path = state_cache_path
        self.__exit_stack: ty.Optional[contextlib.ExitStack] = None

    def __get_state_namespace(self) -> nodestate.StateNamespace:
        """
        Return the state namespace to be used.

        This creates a new object if ``self.state_namespace`` is ``None``.
        """
        if self.state_namespace is not None:
            return self.state_namespace

        if self.use_cached_state:
            cache_path = self.state_cache_path
            if not cache_path:
                cache_path = nodestate.DEFAULT_DB_DIR
            db = nodestate.CachedStateDB(cache_path)
            ns = nodestate.StateNamespace(db)
        else:
            ns = nodestate.StateNamespace()

        return ns

    def __enter__(self) -> YapeContext:
        global _current_context
        if _current_context is not None:
            raise RuntimeError('there is a yape context already in use')

        with contextlib.ExitStack() as exit_stack:
            ns = self.__get_state_namespace()
            exit_stack.enter_context(ns)
            self.__exit_stack = exit_stack.pop_all()

        return self

    def __exit__(self,
                 exc_type: ty.Optional[ty.Type[BaseException]],
                 exc_value: ty.Optional[BaseException],
                 traceback: ty.Optional[types.TracebackType],
                 ) -> ty.Optional[bool]:
        assert self.__exit_stack is not None
        self.__exit_stack.close()
        self.__exit_stack = None
        return None


_current_context: ty.Optional[YapeContext] = None
