"""
TODO: Document this.
"""
import datetime
import json
import pathlib


class Pipeline:
    def __init__(self):
        self.id2unit = {}
        self.units = set()
        self.__unit_id_seq = 0

    def add_unit(self, unit):
        if unit.id and unit.id in self.id2unit:
            raise Exception(f'there is already a unit with the id "{unit.id}"')

        if unit in self.units:
            return

        if not unit.id:
            unit.id = f'unit-{self.__unit_id_seq}'
            self.__unit_id_seq += 1

        self.id2unit[unit.id] = unit
        self.units.add(unit)


class Unit:
    def __init__(self,
        runner,
        id=None,
        dependencies=None,
        runner_conf=None,
        info=None,
    ):
        self.id = id
        self.runner = runner
        self.runner_conf = runner_conf
        self.dependencies = dependencies or {}
        self.info = info

    def dependencies_to_dict(self):
        r = {}
        for name, dep in self.dependencies.items():
            if isinstance(dep, pathlib.Path):
                dep = {'type': 'path', 'path': str(dep)}
            elif isinstance(dep, Unit):
                dep = {'type': 'unit', 'id': dep.id}
            else:
                raise Exception('invalid unit dependency type: {type(dep)}')
            r[name] = dep
        return r

    def __str__(self):
        return f'<{self.runner}:{self.id}>'


class UnitRunner:
    runner_registry = {}

    @classmethod
    def __init_subclass__(cls, **kw):
        cls.__name__
        UnitRunner.runner_registry[cls.__name__] = cls


class UnitWorkspace:
    def __init__(self, unit, pipeline_ws):
        self.unit = unit

        self.dependencies = dict(unit.dependencies)
        for name, dep in self.dependencies.items():
            if isinstance(dep, Unit):
                self.dependencies[name] = pipeline_ws.unit_workspace(dep)

        self.path = pipeline_ws.unit_path(unit)
        self.__state = None

    def state(self):
        if self.__state is not None:
            return self.__state

        self.__state = {
            'last_execution': None,
            'success': None,
            'error_string': None,
            'runner': None,
            'runner_conf': None,
            'dependencies': None,
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

        state['dependencies'] = self.unit.dependencies_to_dict()

        state_path = self.path / 'state.json'
        state_path.parent.mkdir(exist_ok=True, parents=True)
        state_path.write_text(json.dumps(state))

    def is_outdated(self):
        state = self.state()
        if state['last_execution'] is None:
            return True

        if not state['success']:
            return True

        if self.unit.runner != state['runner']:
            return True

        if self.unit.runner_conf != state['runner_conf']:
            return True

        if self.unit.dependencies_to_dict() != state['dependencies']:
            return True

        for name, dep in self.dependencies.items():
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
        self.path = pathlib.Path(path)
        self.pl = pl
        self.unit_workspaces = {}

    def unit_workspace(self, unit):
        if unit in self.unit_workspaces:
            return self.unit_workspaces[unit]

        w = UnitWorkspace(unit, self)
        self.unit_workspaces[unit] = w
        return w

    def unit_path(self, unit):
        return self.path / 'units' / unit.id


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
        execution_list = self.__get_execution_list(targets)

        for unit in execution_list:
            unit_ws = self.workspace.unit_workspace(unit)
            if always_run or unit_ws.is_outdated():
                try:
                    runner_cls = UnitRunner.runner_registry[unit.runner]
                except KeyError:
                    error_msg = f'runner "{unit.runner}" not found. Available runners: '
                    error_msg += ",".join(UnitRunner.runner_registry)
                    raise Exception(error_msg)

                runner = runner_cls()
                runner.unit = unit
                runner.workspace = unit_ws
                try:
                    runner.run()
                except Exception as e:
                    unit_ws.update_state(
                        runner=unit.runner,
                        runner_conf=unit.runner_conf,
                        success=False,
                        last_execution=None,
                        error_string=str(e),
                    )
                    raise e
                else:
                    unit_ws.update_state(
                        runner=unit.runner,
                        runner_conf=unit.runner_conf,
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

    def __get_execution_list(self, targets):
        """
        Return a list of all units to be executed based on target units. Units
        that are direct or indirect dependencies of each target is also added.
        The list is sorted in topological order based on the dependencies.
        """
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

                # Find all dependencies and push back to stack and mark unit as
                # visiting
                deps = [
                    dep for dep in unit.dependencies.values()
                    if isinstance(dep, Unit)
                ]
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
