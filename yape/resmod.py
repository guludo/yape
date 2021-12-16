# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""
Resources module.
"""
from __future__ import annotations

import abc
import types

from . import (
    ty,
)


T = ty.TypeVar('T')


class ResourceRequest(ty.Generic[T]):
    pass


class ResourceProvider(ty.Generic[T], abc.ABC):
    @abc.abstractmethod
    def match(self, request: ResourceRequest[ty.Any]) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def create(self, request: ResourceRequest[T]) -> ty.Any:
        raise NotImplementedError()

    @abc.abstractmethod
    def delete(self, handle: ty.Any) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def exists(self, handle: ty.Any) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def resolve(self, handle: ty.Any) -> T:
        raise NotImplementedError()

    def __enter__(self) -> ResourceProvider[T]:
        _providers_stack.append(self)
        return self

    def __exit__(self,
                 exc_type: ty.Optional[ty.Type[BaseException]],
                 exc_value: ty.Optional[BaseException],
                 traceback: ty.Optional[types.TracebackType],
                 ) -> ty.Optional[bool]:
        _providers_stack.pop()
        return None


_providers_stack: ty.List[ResourceProvider[ty.Any]] = []


def get_provider(request: ResourceRequest[T],) -> ResourceProvider[T]:
    for p in reversed(_providers_stack):
        if p.match(request):
            return p
    raise RuntimeError(f'no provider found for request {request!r}')
