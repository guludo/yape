# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations

import datetime
import hashlib
import os
import pathlib
import pickle
import shutil
import tempfile
import uuid

from . import (
    gn,
    ty,
    walkproto,
)


class State:
    def __init__(self,
                 node: gn.Node,
                 workdir: ty.Union[pathlib.Path, str] = None,
                 ):
        self.node = node
        self.__workdir = pathlib.Path(workdir) if workdir else None
        self.__has_result = False
        self.__result = None

    def has_result(self) -> bool:
        return self.__has_result

    def get_result(self) -> ty.Any:
        if not self.__has_result:
            msg = f'state for node {self.node} has no valid result'
            raise RuntimeError(msg)
        return self.__result

    def set_result(self, result: ty.Any):
        self.__result = result
        self.__has_result = True

    def release(self):
        self.__result = None
        self.__has_result = False

    def is_up_to_date(self) -> bool:
        return False

    def get_timestamp(self) -> ty.Union[datetime.datetime, None]:
        return None

    def workdir(self) -> ty.Union[pathlib.Path, None]:
        if not self.__workdir:
            return None

        self.__workdir.mkdir(exist_ok=True, parents=True)
        return self.__workdir


class CachedState(State):
    def __init__(self,
                 node: gn.Node,
                 path: ty.Union[pathlib.Path, str] = None,
                 node_descriptor_path: ty.Union[pathlib.Path, str] = None,
                 workdir: ty.Union[pathlib.Path, str] = None,
                 check_saved_descriptor: bool = True
                 ):
        self.__path = pathlib.Path(path) if path else None
        self.__cached_is_up_to_date = None
        self.__cached_result_mtime = None
        self.__check_saved_descriptor = check_saved_descriptor

        self.__node_descriptor_path = None
        if node_descriptor_path:
            self.__node_descriptor_path = pathlib.Path(node_descriptor_path)

        if not workdir and self.__path:
            workdir = path / 'workdir'

        super().__init__(node, workdir)

    def __is_up_to_date(self) -> bool:
        # Do checks from the least to the most expensive

        # 1. Check if state directory exists.
        state_dir = self.__path / 'state'
        if not state_dir.is_dir():
            return False

        # 2. Check if any input path has it modification time greater than the
        #    state's timestamp.
        for p in self.node._pathins:
            pathin_mtime = datetime.datetime.fromtimestamp(
                pathlib.Path(p).stat().st_mtime,
                tz=datetime.timezone.utc,
            )
            if pathin_mtime > self.get_timestamp():
                return False

        # 3. Check if nodes this node depends on are up to date (the
        #    node_descriptor of such nodes are by definition smaller than this
        #    node's).
        for dep in self.node._get_dep_nodes():
            dep_state = get_state(dep)
            if not dep_state.is_up_to_date():
                return False
            if dep_state.get_timestamp() > self.get_timestamp():
                return False

        # 4. Finally, if this object was constructed with
        #    ``check_saved_descriptor=True``, compare this node's
        #    node_descriptor with the one saved in the state directory.
        if self.__check_saved_descriptor:
            node_descriptor = self.node._get_node_descriptor()
            node_descriptor_path = self.__node_descriptor_path
            if not node_descriptor_path:
                node_descriptor_path = state_dir / 'node_descriptor.pickle'
            with open(node_descriptor_path, 'rb') as f:
                saved_descriptor = pickle.load(f)
            if node_descriptor != saved_descriptor:
                return False

        return True

    def is_up_to_date(self) -> bool:
        if self.__cached_is_up_to_date is None:
            self.__cached_is_up_to_date = self.__is_up_to_date()
        return self.__cached_is_up_to_date

    def get_timestamp(self) -> datetime.datetime:
        if self.__cached_result_mtime is not None:
            return self.__cached_result_mtime
        result_path = self.__path / 'state' / 'result.pickle'
        result_mtime = datetime.datetime.fromtimestamp(
            result_path.stat().st_mtime,
            tz=datetime.timezone.utc,
        )
        self.__cached_result_mtime = result_mtime
        return result_mtime

    def has_result(self) -> bool:
        if super().has_result():
            return True
        result_path = self.__path / 'state' / 'result.pickle'
        return result_path.exists()

    def get_result(self) -> ty.Any:
        if not super().has_result():
            result_path = self.__path / 'state' / 'result.pickle'
            try:
                with open(result_path, 'rb') as f:
                    result = pickle.load(f)
            except FileNotFoundError:
                pass
            else:
                super().set_result(result)
        return super().get_result()

    def set_result(self, result: ty.Any):
        self.__path.mkdir(exist_ok=True, parents=True)

        tmpdir = pathlib.Path(tempfile.mkdtemp(dir=self.__path))
        try:
            if not self.__node_descriptor_path:
                with open(tmpdir / 'node_descriptor.pickle') as f:
                    node_descriptor = self.node._get_node_descriptor()
                    pickle.dump(node_descriptor, f)

            with open(tmpdir / 'result.pickle', 'wb') as f:
                pickle.dump(result, f)

            if (self.__path / 'state').exists():
                shutil.rmtree(self.__path / 'state')
            os.replace(tmpdir, self.__path / 'state')
        finally:
            # Remove temporary directory if it still exists
            if tmpdir.exists():
                shutil.rmtree(tmpdir)

        super().set_result(result)

    def release(self):
        self.__cached_is_up_to_date = None
        super().release()


class StateNamespace:
    def __init__(self, factory: Callable[[gn.Node], State] = State):
        self.__states = {}
        self.__node_descriptor_cache = {}
        self.factory = factory

    def get_state(self, node: gn.Node) -> State:
        current = self
        if node in self.__states:
            return self.__states[node]
        self.__states[node] = self.factory(node)
        return self.__states[node]

    def __enter__(self):
        global _current_namespace
        if _current_namespace:
            raise RuntimeError('there is already a state namespace in place')
        _current_namespace = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        global _current_namespace
        _current_namespace = None
        self.__cleanup()
        return False

    def __cleanup(self):
        for s in self.__states.values():
            s.release()
        self.__states = {}
        self.__node_descriptor_cache = {}

    def get_node_descriptor(self, node):
        return walkproto.node_descriptor(node, self.__node_descriptor_cache)


class CachedStateDB:
    def __init__(self, path: ty.Union[pathlib.Path, str] = 'db'):
        self.__path = pathlib.Path(path)

    def __call__(self, node: gn.Node) -> State:
        entry_dir = self.__find_entry_dir(node)
        return CachedState(
            node,
            path=entry_dir / 'statedir',
            node_descriptor_path=entry_dir / 'node_descriptor.pickle',
            # We use check_saved_descriptor=False here because we make sure
            # that the saved descriptor matches the node's descriptor in
            # __find_entry_dir()
            check_saved_descriptor=False,
        )

    def __find_entry_dir(self, node: gn.Node) -> pathlib.Path:
        if _current_namespace:
            node_descriptor = _current_namespace.get_node_descriptor(node)
        else:
            node_descriptor = node._get_node_descriptor()
        descriptor_bytes = pickle.dumps(node_descriptor)
        node_hash = hashlib.sha256(descriptor_bytes).hexdigest()

        bucket_dir = self.__path / 'entries' / node_hash
        for entry_dir in bucket_dir.glob('*'):
            with open(entry_dir / 'node_descriptor.pickle', 'rb') as f:
                entry_node_descriptor = pickle.load(f)
            if entry_node_descriptor == node_descriptor:
                return entry_dir

        # Create a new entry
        entry_id = str(uuid.uuid4())
        while (bucket_dir / entry_id).exists():
            entry_id = str(uuid.uuid4())
        entry_dir = bucket_dir / entry_id
        entry_dir.mkdir(exist_ok=True, parents=True)
        with open(entry_dir / 'node_descriptor.pickle', 'wb') as f:
            pickle.dump(node_descriptor, f)

        return entry_dir


def get_state(node: gn.Node) -> State:
    if not _current_namespace:
        raise RuntimeError('not in a state namespace context')
    return _current_namespace.get_state(node)


_current_namespace = None
