"""
TODO: Document this.
"""
import collections
import datetime
import hashlib
import json
import pathlib
import shutil


class Pipeline:
    def __init__(self):
        self.id2unit = {}
        self.output2unit = {}
        self.units = set()
        self.__unit_id_seq = 0
        self.__is_hashed = False

    def add_unit(self, unit):
        if self.__is_hashed:
            raise Exception('pipeline is already hashed, can not add more units')

        if unit.id and unit.id in self.id2unit:
            raise Exception(f'there is already a unit with the id "{unit.id}"')

        for output in unit.outputs.values():
            if output in self.output2unit:
                raise Exception('more than one unit declaring to produce "{output}": {unit} and {self.output2unit[output]}')
            self.output2unit[output] = unit

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

    def get_unit_type_deps(self, unit):
        r = set()
        for dep in unit.deps.values():
            if isinstance(dep, pathlib.Path):
                if dep in self.output2unit:
                    r.add(self.output2unit[dep])
            elif isinstance(dep, Unit):
                r.add(dep)
            elif isinstance(dep, UnitResultDep):
                r.add(dep.unit)
        return r

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
                deps = list(self.get_unit_type_deps(unit))
                stack.append((unit, deps))
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
                # TODO: raise a warning here
                hashes = [f'{h}-{unit.id}' for unit in l]
            for h, unit in zip(hashes, l):
                unit.hash = h

        self.hash2unit = {unit.hash: unit for unit in self.units}

        self.__is_hashed = True

    def __calc_unit_hash(self, unit, cache):
        if unit in cache:
            return cache[unit]

        h = hashlib.sha256()

        # Hash parameters
        params_json = json.dumps(self.__sorted_for_json(unit.params))
        h.update(params_json.encode())

        # Hash dependencies
        for name, dep in unit.deps.items():
            if isinstance(dep, pathlib.Path):
                h.update(str(dep).encode())
            elif isinstance(dep, (Unit, UnitResultDep)):
                if isinstance(dep, UnitResultDep):
                    dep = dep.unit
                h.update(self.__calc_unit_hash(dep, cache).encode())
            else:
                raise Exception('unhandled unit workspace dependency type: {type(dep)}')

        # TODO: consider hashing code as well

        return h.hexdigest()

    def __sorted_for_json(self, obj):
        if isinstance(obj, dict):
            return collections.OrderedDict(
                (k, self.__sorted_for_json(obj[k]))
                for k in sorted(obj.keys())
            )
        elif isinstance(obj, set):
            return sorted(obj)
        else:
            return obj


class Unit:
    def __init__(self,
        runner=None,
        id=None,
        deps=None,
        params=None,
        info=None,
        outputs=None,
        always=False,
    ):
        self.id = id
        self.hash = None
        self.runner = runner
        self.params = params
        self.deps = deps or {}
        self.info = info
        self.outputs = outputs or {}
        self.always = always

        for name, path in self.outputs.items():
            self.outputs[name] = pathlib.Path(path)

    def run(self, ctx):
        if callable(self.runner):
            self.runner(ctx)
        else:
            raise Exception(f'no callable runner available for {self}')

    def deps_to_dict(self):
        r = {}
        for name, dep in self.deps.items():
            if isinstance(dep, pathlib.Path):
                dep = {'type': 'path', 'path': str(dep)}
            elif isinstance(dep, Unit):
                dep = {'type': 'unit', 'hash': dep.hash}
            elif isinstance(dep, UnitResultDep):
                dep = {
                    'type': 'unit_result',
                    'unit_hash': dep.unit.hash,
                    'result_name': dep.name,
                }
            else:
                raise Exception('invalid unit dependency type: {type(dep)}')
            r[name] = dep
        return r

    def __str__(self):
        return f'<{getattr(self.runner, "__name__", "")}:{self.id}>'

    def result(self, name):
        return UnitResultDep(self, name)


class UnitResultDep:
    def __init__(self, unit, name):
        self.unit = unit
        self.name = name


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
            'params': None,
            'deps': None,
        }
        self.kw = kw
        self.load_state()
        self.__result = {}

    def is_outdated(self):
        if self.unit.always:
            return True

        if self.state['timestamp'] is None:
            return True

        if not self.state['success']:
            return True

        if self.unit.params != self.state['params']:
            return True

        if self.unit.deps_to_dict() != self.state['deps']:
            return True

        for name, dep in self.unit.deps.items():
            if isinstance(dep, pathlib.Path):
                t = datetime.datetime.fromtimestamp(dep.stat().st_mtime)
                if t > self.state['timestamp']:
                    return True
            elif isinstance(dep, (Unit, UnitResultDep)):
                dep_unit = dep if isinstance(dep, Unit) else dep.unit
                try:
                    dep_state = self.namespace.get_unit_state(dep_unit)
                except KeyError:
                    raise Exception(f'{self.unit}: state for dependency unit {dep} not found' )
                if dep_state.is_outdated():
                    return True
                if dep_state.state['timestamp'] > self.state['timestamp']:
                    return True
            else:
                raise Exception(f'unhandled unit dependency type: {type(dep)}')

        return False

    def set_result(self, name, value):
        self.__result[name] = value

    def get_result(self, name):
        return self.__result[name]

    def commit(self, **kw):
        self.state.update(kw)
        self.state['params'] = self.unit.params
        self.state['deps'] = self.unit.deps_to_dict()
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

    def save_state(self):
        state = dict(self.state)

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
        p = self.path / 'workdir'
        p.mkdir(exist_ok=True, parents=True)
        return p


class UnitRunnerContext:
    def __init__(self, unit_state):
        self.__unit_state = unit_state

    @property
    def unit(self):
        return self.__unit_state.unit

    @property
    def params(self):
        return self.unit.params

    @property
    def workdir(self):
        return self.__unit_state.workdir()

    def error(self, msg):
        raise Exception(f'{self.unit}: {msg}')

    def dep(self, name):
        if name not in self.unit.deps:
            self.error(f'missing dependency "{name}" for unit {self.unit}')

        dep = self.unit.deps[name]
        if isinstance(dep, pathlib.Path):
            return dep
        elif isinstance(dep, Unit):
            return self.__unit_state.namespace.get_unit_state(dep)
        elif isinstance(dep, UnitResultDep):
            dep_state = self.__unit_state.namespace.get_unit_state(dep.unit)
            return dep_state.get_result(dep.name)
        else:
            self.error(f'unhandled dependency type for "{name}": {type(dep)}')


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
                    if result:
                        for name, value in result:
                            unit.set_result(name, value)
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
                    print(f'{unit} is up to date')


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
