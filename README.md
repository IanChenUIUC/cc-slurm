# About

Yet another template for managing complex jobs submitted into the SLURM scheduler.
Largely vibe-coded, and tested on the Illinois Campus Cluster.

----

The goal is to allow general DAG structures in the jobs.
A format for specifying these as a toml is described, and parsed.

## Quickstart

Using the `just` command runner is easy, and can understand the subcommands.

Seven verbs, each taking an optional glob over node identities:

- default: lists the commands
- dag: the dependency structure the spec expanded into (arrays roll up to one
  line; `verbose=1` lists their tasks)
- run: submits whatever isn't COMPLETED, plus anything downstream
- status: state, elapsed time, and peak RSS per unit (task-level counts;
  `verbose=1` names the tasks of an array that didn't complete)
- history: every logged attempt per unit, not just the latest one
- logs: tails the SLURM and local-run logs
- invalidate / complete: mark units stale / force them to success
- cancel: scancel matching live jobs

Behaviour flags are `just` variable overrides, so they go **before** the verb:
`just dry=1 run 'g'` (exactly what `run` would submit, and why — same decisions,
printed; or, if `g` isn't ready, what it is waiting on), `just deps=1 run 'g'`
(that plus whatever upstream it still needs), `just local=1 run 'g'` (synchronous,
no SLURM), `just force=1 run 'g'` (redo it and its downstream),
`just force=1 only=1 run 'g'` (redo it alone), `just verbose=1 status`.
See `spec.md` §11 for which one to reach for.

The `template.zip` contains the files that can be copied into any project.
```
template
├── cc-local
├── cc-submit
├── justfile
├── pipeline.py
└── pipeline.toml
```
so that new projects can, for example, start with
```
wget https://github.com/IanChenUIUC/cc-slurm/raw/refs/heads/main/template.zip &&
unzip -j template.zip -d slurm && rm template.zip
```

## Overview

There are three different systems in play:

1. The user machine, which will execute commands remotely on the login node (via `ssh cc`)
2. The login node, which is setup via SLURM to run `sbatch` and `sarray` commands.
3. The compute nodes, in which jobs are submitted.

On the user machine, the dependencies for running `cc-submit` and `pipeline.py` must be present.
On the login node, the `array.sbatch.sh` and `run.sbatch.sh` must be present in the `SLURM_DIRECTORY`, as specified in `cc-submit`.
Finally, the compute node must have the `CONTAINER`, as specified in the `*.sbatch.sh` files above.

For short testing runs on the login node, entering the `CONTIANER`, running `just local=1 run` will bypass the SLURM scheduler and run jobs directly.
Otherwise, figuring out what will be run using `just dry=1 run` and `just dag` commands, and then `just status`.

## How commands are materialized

`just dry=1 run`/`just run` write the resolved, per-node `command` under `.pipeline/scripts/`:

- **Individual jobs** → one script `<node>`, uploaded and run by `run.sbatch.sh`.
- **Array recipes** (`array = true`) → a directory `<recipe>.tasks/` holding one
  script per task, `task-<idx>`, where `idx` is the node's array index. The
  whole directory is uploaded and `array.sbatch.sh` runs task *i* via
  `<dir>/task-$SLURM_ARRAY_TASK_ID`.

Each script is a **self-contained executable** (mode 0755) whose first line is a
shebang: `#!/bin/bash` plus `set -euo pipefail` by default, or `#!<interpreter>` when
the recipe declares one — so a body can be written in Python (or anything else) via
`command_file` + `interpreter`, and the wrapper scripts exec it without knowing the
language. Hence no file extension: the shebang is the single source of truth.

Because every task is its own script, a `command` may span multiple lines (multiple
statements, heredocs, `\`-continuations) and runs intact — the same as an individual
job. Accordingly, `cc-submit array` takes the **tasks directory** (not a one-line-per-command
file) and sizes `--array` from the number of `task-*.sh` scripts in it.

## Tests

Dev-only; not shipped in the template (`gen-template.sh` copies just the four runners).

```
uv run pytest
```

The engine is exercised against a **mocked pipeline state** — no cluster. `tests/mockpipe.py`
is dual-use: a library the tests import (`mock_run`) and a by-hand "is this fixed?" checker,
e.g.

```
python3 tests/mockpipe.py --spec path/to/pipeline.toml \
    --state genquery=RUNNING --state ib-core-decomp=COMPLETED \
    --run "submit --only testing-csk*"
```

which seeds the run log + a fake `sacct`/`cc-submit` in a throwaway workdir and prints what
the engine would submit (and the dependencies it would attach). See `tests/COVERAGE.md` for
the spec-section → test ledger.
