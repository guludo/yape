# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""
Utility module to contain common typing-related utilities. Members of the
typing module should be exported by this module and be used as this module's
attributes. This allows some eventual API incompatibilities between Python
versions to be treated in a single place.
"""
from typing import (
    Any as Any,
    Callable as Callable,
    ContextManager as ContextManager,
    Dict as Dict,
    Generator as Generator,
    Generic as Generic,
    Iterable as Iterable,
    List as List,
    Mapping as Mapping,
    NamedTuple as NamedTuple,
    NewType as NewType,
    NoReturn as NoReturn,
    Optional as Optional,
    Protocol as Protocol,
    Sequence as Sequence,
    Set as Set,
    Tuple as Tuple,
    TypeVar as TypeVar,
    Type as Type,
    Union as Union,
    cast as cast,
    get_args as get_args,
    overload as overload,
)
