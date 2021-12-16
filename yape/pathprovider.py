# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
import pathlib
import uuid
import shutil

from . import (
    resmod,
    ty,
)


DEFAULT_PATH_PROVIDER_BASE = pathlib.Path('.yape', 'paths')


class PathRequest(resmod.ResourceRequest[pathlib.Path]):
    pass


class PathProvider(resmod.ResourceProvider[pathlib.Path]):
    def __init__(self,
                 base: ty.Union[pathlib.Path, str] = DEFAULT_PATH_PROVIDER_BASE,
                 ):
        self.__base = pathlib.Path(base)

    def match(self, request: resmod.ResourceRequest[ty.Any]) -> bool:
        return isinstance(request, PathRequest)

    def create(self, request: resmod.ResourceRequest[pathlib.Path]) -> ty.Any:
        assert isinstance(request, PathRequest)
        d = self.__entries_dir(create=True)
        while True:
            handle = str(uuid.uuid4())
            resource_dir = d / handle
            try:
                resource_dir.mkdir()
            except FileExistsError:
                continue
            else:
                break
        return handle

    def delete(self, handle: ty.Any) -> None:
        resource_dir = self.__entries_dir() / ty.cast(str, handle)
        shutil.rmtree(resource_dir)

    def exists(self, handle: ty.Any) -> bool:
        resource_dir = self.__entries_dir() / ty.cast(str, handle)
        return resource_dir.exists()

    def resolve(self, handle: ty.Any) -> pathlib.Path:
        resource_dir = self.__entries_dir() / ty.cast(str, handle)
        return resource_dir / 'resource'

    def __entries_dir(self, create: bool = False) -> pathlib.Path:
        d = self.__base / 'entries'
        if create:
            d.mkdir(exist_ok=True, parents=True)
        return d
