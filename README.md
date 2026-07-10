# About

Yet another template for managing complex jobs submitted into the SLURM scheduler.
Largely vibe-coded, and tested on the Illinois Campus Cluster.

----

The goal is to allow general DAG structures in the jobs.
A format for specifying these as a toml is described, and parsed.

## Quickstart

Using the `just` command runner is easy, and can understand the subcommands.

- default: lists the commands
- dry: prints out the commands that are run
- dag: prints out the dependency structure
- run: submits all jobs
- status: shows the status of all jobs

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

For short testing runs on the login node, entering the `CONTIANER`, running `just local` with bypass the SLURM scheduler and run jobs directly.
Otherwise, figuring out what will be run using `just dry` and `just dag` commands, and then `just status`.

## How commands are materialized

`just dry`/`just run` write the resolved, per-node `command` under `.pipeline/scripts/`:

- **Individual jobs** → one script `<node>.sh`, uploaded and run by `run.sbatch.sh` (`bash <script>`).
- **Array recipes** (`array = true`) → a directory `<recipe>.tasks/` holding one
  script per task, `task-<idx>.sh`, where `idx` is the node's array index. The
  whole directory is uploaded and `array.sbatch.sh` runs task *i* via
  `bash <dir>/task-$SLURM_ARRAY_TASK_ID.sh`.

Because every task is its own script, a `command` may span multiple lines (multiple
statements, heredocs, `\`-continuations) and runs intact — the same as an individual
job. Accordingly, `cc-submit array` takes the **tasks directory** (not a one-line-per-command
file) and sizes `--array` from the number of `task-*.sh` scripts in it.
