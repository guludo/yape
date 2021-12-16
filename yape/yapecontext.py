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
    pathprovider,
    resmod,
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
                 resource_providers:
                     ty.Optional[ty.List[resmod.ResourceProvider[ty.Any]]] = None,
                 use_path_provider: bool = True,
                 path_provider_base: ty.Union[str, pathlib.Path, None] = None,
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

        :param resource_providers: a list of resource providers to be used in
          this context. When looking for a provider for a resource request, the
          search will be from the first provider in the list throught the last.
          The first that matches the request will be used.

        :param use_path_provider: if true (the default), then a default path
          provider is created and appended to the list of resource providers
          for this context.

        :param path_provider_base: path to the base directory for the default
          path provider. This is only applicable if ``use_path_provider`` is
          true. If omitted, then
          ``yape.pathprovider.DEFAULT_PATH_PROVIDER_BASE`` is used.
        """
        self.state_namespace = state_namespace
        self.use_cached_state = use_cached_state
        self.state_cache_path = state_cache_path
        self.resource_providers = resource_providers
        self.use_path_provider = use_path_provider
        self.path_provider_base = path_provider_base

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

    def __get_resource_providers(self,
                                 ) -> ty.List[resmod.ResourceProvider[ty.Any]]:
        """
        Return the list of resource providers to be used.
        """
        r: ty.List[resmod.ResourceProvider[ty.Any]] = []
        if self.resource_providers:
            r.extend(self.resource_providers)
        if self.use_path_provider:
            base = self.path_provider_base
            if not base:
                base = pathprovider.DEFAULT_PATH_PROVIDER_BASE
            r.append(pathprovider.PathProvider(base))
        return r

    def __enter__(self) -> YapeContext:
        global _current_context
        if _current_context is not None:
            raise RuntimeError('there is a yape context already in use')

        with contextlib.ExitStack() as exit_stack:
            ns = self.__get_state_namespace()
            exit_stack.enter_context(ns)
            for p in reversed(self.__get_resource_providers()):
                exit_stack.enter_context(p)
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
