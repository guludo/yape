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

    def add_unit(self, unit):
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
        self.runner = runner
        self.params = params
        self.deps = deps or {}
        self.info = info
        self.outputs = outputs or {}
        self.always = always

        for name, path in self.outputs.items():
            self.outputs[name] = pathlib.Path(path)

    def run(self, unit_ws):
        if callable(self.runner):
            self.runner(unit_ws)
        else:
            raise Exception(f'no callable runner available for {self}')

    def __str__(self):
        return f'<{getattr(self.runner, "__name__", "")}:{self.id}>'


class UnitWorkspace:
    def __init__(self, unit):
        self.unit = unit

        # These properties are set by PipelineWorkspace later
        self.id = None
        self.path = None
        self.deps = None
        self.params = unit.params
        self.outputs = unit.outputs

        self.__state = None
        self.__hash = None

    def hash(self):
        if self.__hash:
            return self.__hash

        h = hashlib.sha256()

        # Hash parameters
        params_json = json.dumps(self.__sorted_for_json(self.unit.params))
        h.update(params_json.encode())

        # Hash dependencies
        for name, dep in self.deps.items():
            if isinstance(dep, pathlib.Path):
                h.update(str(dep).encode())
            elif isinstance(dep, UnitWorkspace):
                h.update(dep.hash().encode())
            else:
                raise Exception('unhandled unit workspace dependency type: {type(dep)}')

        # TODO: consider hashing code as well

        self.__hash = h.hexdigest()

        return self.__hash

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

    def state(self):
        if self.__state is not None:
            return self.__state

        self.__state = {
            'last_execution': None,
            'success': None,
            'error_string': None,
            'params': None,
            'deps': None,
        }

        state_path = self.path / 'state.json'
        if not state_path.exists():
            return self.__state

        self.__state = json.loads(state_path.read_text())

        if self.__state['last_execution']:
            t = datetime.datetime.fromisoformat(self.__state['last_execution'])
            self.__state['last_execution'] = t

        return self.__state

    def update_state(self, **kw):
        for k in self.state():
            if k in kw:
                self.__state[k] = kw[k]
        self.save_state()

    def save_state(self):
        state = dict(self.state())

        if state['last_execution']:
            state['last_execution'] = state['last_execution'].isoformat()

        state['deps'] = self.deps_to_dict()

        state['runner'] = getattr(self.unit.runner, '__name__', '')

        state_path = self.path / 'state.json'
        state_path.parent.mkdir(exist_ok=True, parents=True)
        state_path.write_text(json.dumps(state))

    def deps_to_dict(self):
        r = {}
        for name, dep in self.deps.items():
            if isinstance(dep, pathlib.Path):
                dep = {'type': 'path', 'path': str(dep)}
            elif isinstance(dep, UnitWorkspace):
                dep = {'type': 'unit_ws', 'id': dep.id}
            else:
                raise Exception('invalid unit dependency type: {type(dep)}')
            r[name] = dep
        return r


    def is_outdated(self):
        if self.unit.always:
            return True

        state = self.state()
        if state['last_execution'] is None:
            return True

        if not state['success']:
            return True

        if self.unit.params != state['params']:
            return True

        if self.deps_to_dict() != state['deps']:
            return True

        for name, dep in self.deps.items():
            if isinstance(dep, pathlib.Path):
                t = datetime.datetime.fromtimestamp(dep.stat().st_mtime)
                if t > state['last_execution']:
                    return True
            elif isinstance(dep, UnitWorkspace):
                if dep.is_outdated():
                    return True
                if dep.state()['last_execution'] > state['last_execution']:
                    return True

        return False


class PipelineWorkspace:
    def __init__(self, pl, path):
        pl.topological_sort() # Make sure there are no cycles
        self.path = pathlib.Path(path)
        self.pl = pl
        self.unit_workspaces = {}

        for unit in pl.units:
            self.unit_workspaces[unit] = UnitWorkspace(unit)

        # Convert dependencies
        for unit_ws in self.unit_workspaces.values():
            unit_ws.deps = dict(unit_ws.unit.deps)
            for name, dep in unit_ws.deps.items():
                if isinstance(dep, Unit):
                    unit_ws.deps[name] = self.unit_workspaces[dep]

        # Generate unit workspace ids from hash values
        hash2unit_ws = collections.defaultdict(list)
        for unit_ws in self.unit_workspaces.values():
            hash2unit_ws[unit_ws.hash()].append(unit_ws)

        for h, l in hash2unit_ws.items():
            ws_ids = [h]
            if len(l) > 1:
                # In case of collisions, lets use the unit id for
                # disambiguation
                # TODO: raise a warning here
                ws_ids = [f'{h}-{unit_ws.unit.id}' for unit_ws in l]

            for ws_id, unit_ws in zip(ws_ids, l):
                unit_ws.id = ws_id
                unit_ws.path = self.path / 'uws' / ws_id

    def garbage_collect(self):
        active_ids = set(unit_ws.id for unit_ws in self.unit_workspaces.values())
        all_ids = set(p.name for p in (self.path / 'uws').glob('*'))

        to_remove = all_ids - active_ids
        for ws_id in to_remove:
            shutil.rmtree(self.path / 'uws' / ws_id)


class PipelineRunner:
    def __init__(self, pl, workspace):
        self.workspace = PipelineWorkspace(pl, workspace)
        self.pl = pl

    def run(self, targets=None, always_run=False):
        self.workspace.path.mkdir(parents=True, exist_ok=True)

        if targets is None:
            targets = list(self.pl.units)

        targets = self.__parse_targets(targets)
        target_set = set(targets)
        execution_list = self.pl.topological_sort(targets)

        for unit in execution_list:
            unit_ws = self.workspace.unit_workspaces[unit]
            if always_run or unit_ws.is_outdated():
                try:
                    unit.run(unit_ws)
                except Exception as e:
                    unit_ws.update_state(
                        params=unit.params,
                        success=False,
                        last_execution=None,
                        error_string=str(e),
                    )
                    raise e
                else:
                    unit_ws.update_state(
                        params=unit.params,
                        success=True,
                        last_execution=datetime.datetime.now(),
                        error_string=None,
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
