Pulpsolv
--------

A proof of concept of a `libsolv`-based content unit dependency solving Pulp module.

Currently Pulp uses its own mechanism to solve dependencies of units when e.g copying
the units between repositories.

This script does compute what dependencies are needed to copy a single rpm unit from a specified
source repository to the target repository.

Usage
-----

The script has to be run on a working Pulp server.

```
python pulpsolv.py --source-repo zoo --unit-name penguin --target-repo foo
```
