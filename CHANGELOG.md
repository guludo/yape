# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Changed
- **BREAKING**: Renamed parameter `graph` of `Node` constructor to `parent`.
    That way, we have both `Node` and `Graph` using the same parameter name
    for the graph containing them.
- **BREAKING**: Nodes and graphs have the global graph as parent by default
    now. `no_parent=True` must be used if it is desired for the object not to
    have a parent.
- **BREAKING**:
  - `Runner` (and consequently `yape.run()`) use the global graph by
    default now.
  - `MingraphBuilder` (and consequently `yape.mingraph()`) use the global
    graph by default now.

## 0.1.0 - 2021-10-14
### Added
- First release!
