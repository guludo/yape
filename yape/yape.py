"""
TODO: Document this.
"""
import collections
import datetime
import hashlib
import json
import pathlib
import pickle
import shutil
import logging


logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self):
        self.id2unit = {}
        self.pathout2unit = {}
        self.units = set()
        self.__unit_id_seq = 0
        self.__is_hashed = False

    def add_unit(self, unit):
        if self.__is_hashed:
            raise Exception('pipeline is already hashed, can not add more units')

        if unit.id and unit.id in self.id2unit:
            raise Exception(f'there is already a unit with the id "{unit.id}"')

        # Check for PathOut instances
        for v in unit.input_values():
            if not isinstance(v, PathOut):
                continue

            if v in self.pathout2unit:
                raise Exception('more than one unit declaring to produce "{output}": {unit} and {self.pathout2unit[output]}')
            self.pathout2unit[v] = unit

        if unit in self.units:
            return

        if not unit.id:
            unit.id = f'unit-{self.__unit_id_seq}'
            self.__unit_id_seq += 1

        self.id2unit[unit.id] = unit
        self.units.add(unit)

    def unit(self, **kw):
        unit = Unit(**kw)
        self.add_unit(unit)
        return unit

    def __get_unit_deps_from_value(self, value, visited):
        if id(value) in visited:
            return

        visited.add(id(value))

        if isinstance(value, PathIn):
            dep = self.pathout2unit.get(PathOut(value), None)
            if dep:
                yield dep
        elif isinstance(value, Unit):
            yield value
        elif isinstance(value, UnitResultSubscript):
            yield value.unit
        elif isinstance(value, (tuple, list)):
            for v in value:
                yield from self.__get_unit_deps_from_value(v, visited)
        elif isinstance(value, dict):
            for v in value.values():
                yield from self.__get_unit_deps_from_value(v, visited)


    def unit_dependencies(self, unit):
        visited = set()
        return set(
            dep
            for deps in (
                self.__get_unit_deps_from_value(v, visited)
                for v in unit.input_values()
            )
            for dep in deps
        )

    def topological_sort(self, targets=None):
        """
        Return a list of all units to be executed based on target units. Units
        that are direct or indirect dependencies of each target is also added.
        The list is sorted in topological order based on the dependencies.
        """
        if targets is None:
            targets = list(self.units)

        visited = set()
        visiting = set()
        execution_list = []
        path = []
        stack = [(unit, None) for unit in targets]
        while stack:
            unit, state = stack.pop()

            if state is None:
                if unit in visited:
                    # This unit and dependencies have already been added to the
                    # execution list
                    continue

                path.append(unit)

                if unit in visiting:
                    # If I am still visiting this unit, this means a circular
                    # dependency has been found
                    path_ids = [unit.id for u in path]
                    path_ids.reverse()
                    raise Exception(f'circular dependency found between units: {" <- ".join(path_ids)}')

                # Get unit-type dependencies and push back to stack and mark
                # unit as visiting
                stack.append((unit, list(self.unit_dependencies(unit))))
                visiting.add(unit)
            else:
                deps = state
                # If there are no more dependencies to be added to the
                # execution list, the unit is ready to be added. If not, then
                # add the next unit to the stack.
                if not deps:
                    execution_list.append(unit)
                    visited.add(unit)
                    visiting.remove(unit)
                    path.pop()
                else:
                    dep = deps.pop()
                    stack.append((unit, deps))
                    stack.append((dep, None))

        return execution_list

    def calculate_hashes(self):
        if self.__is_hashed:
            return
        # Ensure there are no cycles by doing a topological sort
        self.topological_sort()

        hash2unit_list = collections.defaultdict(list)
        hash_cache = {}
        for unit in self.units:
            h = self.__calc_unit_hash(unit, hash_cache)
            hash2unit_list[h].append(unit)

        # Attach hash values to units.
        for h, l in hash2unit_list.items():
            hashes = [h]
            # Solve collisions by appending the unit id.
            if len(l) > 1:
                logging.warning(f'units [{", ".join(str(u) for u in l)}] have colliding hashes')
                hashes = [f'{h}-{unit.id}' for unit in l]
            for h, unit in zip(hashes, l):
                unit.hash = h

        self.hash2unit = {unit.hash: unit for unit in self.units}

        self.__is_hashed = True

    def __json_default(self, o):
        if isinstance(o, set):
            return sorted(o)
        if isinstance(o, (PathIn, PathOut)):
            return str(o)
        return str(o)

    def __calc_unit_hash(self, unit, cache):
        if unit in cache:
            return cache[unit]

        h = hashlib.sha256()

        # Hash input
        input_json = json.dumps(unit.input_to_dict(), default=self.__json_default)
        h.update(input_json.encode())

        # TODO: consider hashing code as well

        return h.hexdigest()


class Unit:
    def __init__(self,
        runner=None,
        id=None,
        args=None,
        kw=None,
        info=None,
        always=False,
    ):
        self.id = id
        self.hash = None
        self.runner = runner
        self.args = args or tuple()
        self.kw = kw or {}
        self.info = info or {}
        self.always = always

    def run(self, ctx):
        args, kw = ctx.resolve_input(self.args, self.kw)
        if callable(self.runner):
            return self.runner(*args, **kw)
        else:
            raise Exception(f'no callable runner available for {self}')

    def __input_value_to_dict(self, value, cache):
        if id(value) in cache:
            return {'type': 'ref', 'ref_idx': cache[id(value)]}
        else:
            cache[id(value)] = len(cache)

        if isinstance(value, PathOut):
            return {'type': 'pathout', 'path': str(value)}
        elif isinstance(value, PathIn):
            return {'type': 'pathin', 'path': str(value)}
        elif isinstance(value, Unit):
            return {'type': 'unit', 'hash': value.hash}
        elif isinstance(value, UnitResultSubscript):
            return {
                'type': 'unit_result',
                'unit_hash': value.unit.hash,
                'result_name': value.key,
            }
        elif isinstance(value, (list, tuple)):
            return {
                'type': 'list' if isinstance(value, list) else 'tuple',
                'values': [
                    self.__input_value_to_dict(v, cache)
                    for v in value
                ],
            }
        elif isinstance(value, dict):
            keys = sorted(value)
            return {
                'type': 'dict',
                'keys': keys,
                'values': [
                    self.__input_value_to_dict(value[k], cache)
                    for k in keys
                ],
            }
        elif value is CTX:
            return {'type': 'ctx'}
        else:
            return {'type': 'other', 'value': value}

    def input_to_dict(self):
        r = {'args': [], 'kw': {}}

        cache = {}
        for v in self.args:
            r['args'].append(self.__input_value_to_dict(v, cache))

        for k, v in self.kw.items():
            r['kw'][k] = self.__input_value_to_dict(v, cache)

        return r

    def input_values(self):
        yield from self.args
        yield from self.kw.values()

    def __str__(self):
        return f'<{getattr(self.runner, "__name__", "")}:{self.id}>'

    def __getitem__(self, key):
        return UnitResultSubscript(self, name)


class PathIn(pathlib.PurePosixPath):
    pass


class PathOut(pathlib.PurePosixPath):
    pass

CTX = object()

class UnitResultSubscript:
    def __init__(self, unit, key):
        self.unit = unit
        self.key = name


class UnitStateNamespace:
    def __init__(self, factory=None):
        self.__states = {}
        self.factory = factory or InMemoryUnitState

    def get_unit_state(self, unit):
        if unit in self.__states:
            return self.__states[unit]
        self.__states[unit] = self.factory(unit, self)
        return self.__states[unit]


class UnitState:
    def __init__(self, unit, namespace, **kw):
        self.unit = unit
        self.namespace = namespace
        self.state = {
            'success': None,
            'timestamp': None,
            'error_string': None,
            'input': None,
        }
        self.kw = kw
        self.load_state()
        self.__result = None

    def is_outdated(self):
        if self.unit.always:
            return True

        if self.state['timestamp'] is None:
            return True

        if not self.state['success']:
            return True

        if not self.has_result():
            return True

        if self.unit.input_to_dict() != self.state['input']:
            return True

        for value in self.unit.input_values():
            if isinstance(value, PathIn):
                p = pathlib.Path(value)
                t = datetime.datetime.fromtimestamp(p.stat().st_mtime)
                if t > self.state['timestamp']:
                    return True
            elif isinstance(value, (Unit, UnitResultSubscript)):
                unit = value if isinstance(value, Unit) else value.unit
                try:
                    dep_state = self.namespace.get_unit_state(unit)
                except KeyError:
                    raise Exception(f'{self.unit}: state for dependency unit {unit} not found' )
                if dep_state.is_outdated():
                    return True
                if dep_state.state['timestamp'] > self.state['timestamp']:
                    return True

        return False

    def has_result(self):
        return True

    def set_result(self, result):
        self.__result = result

    def get_result(self):
        return self.__result

    def commit(self, **kw):
        self.state.update(kw)
        self.state['input'] = self.unit.input_to_dict()
        self.state['timestamp'] = datetime.datetime.now()
        self.save_state()

    def save_state(self):
        pass

    def load_state(self):
        pass

    def workdir(self):
        return None


class InMemoryUnitState(UnitState):
    pass


class FSUnitState(UnitState):
    class Workspace:
        def __init__(self, path):
            self.path = pathlib.Path(path)

        def __call__(self, unit, namespace):
            return FSUnitState(unit, namespace, path=self.path / unit.hash)

        def garbage_collect(self, units):
            active_hashes = set(unit.hash for unit in units)
            all_hashes = set(p.name for p in self.path.glob('*'))

            to_remove = all_hashes - active_hashes
            for h in to_remove:
                shutil.rmtree(self.path / h)

    def __init__(self, *k, **kw):
        super().__init__(*k, **kw)
        self.__result = None
        self.__result_loaded = False

    def has_result(self):
        if 'fsunitstate_result_state' not in self.state:
            return False

        return self.state['fsunitstate_result_state']['valid']

    def set_result(self, result):
        self.__result = result
        self.__result_loaded = True

    def get_result(self):
        if self.__result_loaded:
            return self.__result

        self.__load_result()
        return self.__result

    def __load_result(self):
        with open(self.kw['path'] / 'result.pickle', 'rb') as f:
            self.__result = pickle.load(f)
            self.__result_loaded = True

    def __save_result(self):
        result_path = self.kw['path'] / 'result.pickle'
        result_path.parent.mkdir(exist_ok=True, parents=True)
        with open(result_path, 'wb') as f:
            pickle.dump(self.__result, f)

    def save_state(self):
        state = dict(self.state)

        if state['success']:
            try:
                self.__save_result()
            except Exception as e:
                state['fsunitstate_result_state'] = {
                    'valid': False,
                    'error_string': str(e),
                }
                logger.warning(f'unable to save result of {self.unit}:\n{e}')
            else:
                state['fsunitstate_result_state'] = {
                    'valid': True,
                }
        else:
            state['fsunitstate_result_state'] = {
                'valid': False,
            }

        if state['timestamp']:
            state['timestamp'] = state['timestamp'].isoformat()

        state_path = self.kw['path'] / 'state.json'
        state_path.parent.mkdir(exist_ok=True, parents=True)
        state_path.write_text(json.dumps(state))

    def load_state(self):
        state_path = self.kw['path'] / 'state.json'
        if not state_path.exists():
            return

        state = json.loads(state_path.read_text())
        if state['timestamp']:
            t = datetime.datetime.fromisoformat(state['timestamp'])
            state['timestamp'] = t
        self.state = state

    def workdir(self):
        p = self.kw['path'] / 'workdir'
        p.mkdir(exist_ok=True, parents=True)
        return p


class UnitRunnerContext:
    def __init__(self, unit_state):
        self.__unit_state = unit_state

    @property
    def unit(self):
        return self.__unit_state.unit

    @property
    def workdir(self):
        return self.__unit_state.workdir()

    def error(self, msg):
        raise Exception(f'{self.unit}: {msg}')

    def __resolve_input_value(self, value, cache):
        if id(value) in cache:
            return cache[id(value)]

        if isinstance(value, Unit):
            r = self.__unit_state.namespace.get_unit_state(value).get_result()
        elif isinstance(value, UnitResultSubscript):
            dep_state = self.__unit_state.namespace.get_unit_state(value.unit)
            result = dep_state.get_result()
            try:
                r = result[value.key]
            except KeyError:
                self.error(f'{value.unit}\'s result does not have the key: {value.key}')
            except TypeError as e:
                self.error(f'incompatible type for {value.unit}\'s result: {e}')
        elif isinstance(value, (PathIn, PathOut)):
            r = pathlib.Path(value)
        elif isinstance(value, list):
            r = [self.__resolve_input_value(v, cache) for v in value]
        elif isinstance(value, tuple):
            r = tuple(self.__resolve_input_value(v, cache) for v in value)
        elif isinstance(value, dict):
            r = {
                k: self.__resolve_input_value(v, cache)
                for k, v in value.items()
            }
        elif value is CTX:
            r = self
        else:
            r = value

        cache[id(value)] = r
        return r

    def resolve_input(self, args, kw):
        cache = {}
        args = tuple(
            self.__resolve_input_value(v, cache) for v in args
        )
        kw = {k: self.__resolve_input_value(v, cache) for k, v in kw.items()}
        return args, kw


class PipelineRunner:
    def __init__(self, pl, ns=None):
        self.ns = ns or UnitStateNamespace()
        self.pl = pl

    def run(self, targets=None, always_run=False):
        self.pl.calculate_hashes()
        if targets is None:
            targets = list(self.pl.units)

        targets = self.__parse_targets(targets)
        target_set = set(targets)
        execution_list = self.pl.topological_sort(targets)

        for unit in execution_list:
            unit_state = self.ns.get_unit_state(unit)
            if always_run or unit_state.is_outdated():
                try:
                    ctx = UnitRunnerContext(unit_state)
                    result = unit.run(ctx)
                    unit_state.set_result(result)
                except Exception as e:
                    unit_state.commit(
                        success=False,
                        error_string=str(e),
                    )
                    raise e
                else:
                    unit_state.commit(
                        success=True,
                    )
            else:
                if unit in target_set:
                    logger.info(f'{unit} is up to date')


    def __parse_targets(self, targets):
        """
        Normalize targets: make them a list and replace strings representing
        ids with the corresponding Unit object.
        """
        targets = list(targets)
        for i, target in enumerate(targets):
            if not isinstance(target, (str, Unit)):
                raise Exception(f'invalid type for target {target}: {type(target)}')

            if isinstance(target, str):
                try:
                    target = self.pl.id2unit[target]
                except KeyError:
                    raise Exception(f'unit with id "{target}" not found in the pipeline')

            targets[i] = target
        return targets
