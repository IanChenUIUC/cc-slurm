set shell := ["bash", "-euo", "pipefail", "-c"]

# Every verb takes an optional GLOB matching node identities, e.g. `just run 'testing-*'`.
#
# Behaviour flags are `just` variable overrides, so they go BEFORE the verb:
#
#   just dry=1     run 'testing-*'      # what it would submit, and why: same decisions,
#                                       #   printed instead of issued
#   just local=1   run 'testing-*'      # here, synchronously, via cc-local (no SLURM)
#   just force=1   run 'testing-*'      # the inputs or the code changed: redo these
#                                       #   AND everything downstream
#   just force=1 only=1 run 'testing-*' # the job flaked: redo these alone and leave
#                                       #   downstream results in place
#   just deps=1    run 'testing-*'      # these, plus whatever upstream they still need
#   just verbose=1 status               # expand rolled-up recipes and name failed tasks
#   just spec=other.toml dag            # any variable below can be overridden this way
#
# (`just` only accepts overrides ahead of the recipe name; anything after it is a
# positional argument, so `just run 'g' local=1` would be read as the spec path.)

local   := ""
force   := ""
only    := ""
deps    := ""
dry     := ""
verbose := ""

spec       := "pipeline.toml"
cc_submit  := "./cc-submit"
cc_local   := "./cc-local"
ssh        := "ssh cc"
sacct      := ssh + " sacct"
scancel    := ssh + " scancel"
workdir    := ".pipeline"
slurmlog   := "/scratch/ianchen3/slurm"

# How the runner is selected. `--local` also changes what state means, so `status`
# takes it too.
runner  := if local == "" { "--cc-submit '" + cc_submit + "' --sacct '" + sacct + "'" } \
           else { "--cc-submit '" + cc_local + "' --local" }

default:
    @just --list

# ---- inspection: no cluster, no side effects -------------------------------

# Reach for it after editing the spec, to see what it expanded into. Arrays print
# as one line each; `just verbose=1 dag` lists their tasks.

# The resolved DAG: units, edges, and dependency types.
dag glob='*':
    python3 pipeline.py dag {{spec}} '{{glob}}' {{ if verbose == '' { '' } else { '-v' } }}

# Multi-unit recipes roll up to one line, counting *tasks* rather than units, so a
# 3-task failure inside a 240-task array is visible. `just verbose=1 status` expands
# to one row per unit and names the tasks that did not complete.

# Each unit's state, elapsed time, and peak RSS.
status glob='*':
    python3 pipeline.py status {{spec}} '{{glob}}' --workdir '{{workdir}}' --sacct '{{sacct}}' \
        {{ if verbose == '' { '' } else { '-v' } }} {{ if local == '' { '' } else { '--local' } }}

# Reach for it when `status` isn't enough because the interesting attempt is not
# the latest one — a unit that timed out, was repaired, and now reads COMPLETED.

# Every logged attempt per unit: when, what happened, job id, state, RSS.
history glob='*':
    python3 pipeline.py history {{spec}} '{{glob}}' --workdir '{{workdir}}'

# ---- running ---------------------------------------------------------------

# See the flag notes at the top of this file for local / force / only / deps / dry.
# `dry=1` is the one to reach for before any run you're unsure about: same reconcile
# and same decisions, printed instead of issued, and it still materializes the job
# scripts into the state dir so you can read them.

# Submit whatever isn't already COMPLETED, plus anything downstream of it.
run glob='*':
    python3 pipeline.py submit {{spec}} --workdir '{{workdir}}' {{runner}} \
        {{ if dry == '' { '' } else { '--dry' } }} {{ if deps == '' { '' } else { '--deps' } }} \
        {{ if force == '' { "--only '" + glob + "'" } \
           else { if only == '' { "--rerun '" + glob + "'" } \
                  else { "--only '" + glob + "' --rerun '" + glob + "'" } } }}

# ---- state -----------------------------------------------------------------

# Reach for it when you know something is wrong but aren't ready to rerun now
# (`just force=1 run` is the do-it-now version).

# Mark units stale; the next `run` redoes them and their downstream.
invalidate glob:
    python3 pipeline.py invalidate {{spec}} '{{glob}}' --workdir '{{workdir}}'

# Reach for it when you re-ran the work by hand outside the pipeline and just
# need the DAG to agree. Undone by a later `invalidate`.

# Force units to COMPLETED.
complete glob:
    python3 pipeline.py complete {{spec}} '{{glob}}' --workdir '{{workdir}}'

# ---- utilities -------------------------------------------------------------

# Remote logs are slurm-<jobid>.out (arrays: slurm-<jobid>_<idx>.out), so GLOB is
# mapped to job-id patterns via `log-ids` and tailed over ssh.

# Tail the remote SLURM logs and any local-run logs for matching units.
logs glob='*':
    @patterns="$(python3 pipeline.py log-ids {{spec}} '{{glob}}' --workdir '{{workdir}}' | tr '\n' ' ')"; \
     found=0; \
     if [ -n "${patterns// /}" ]; then \
       {{ssh}} "cd {{slurmlog}} && tail -n +1 $patterns" && found=1 || true; \
     fi; \
     if compgen -G "{{workdir}}/local-logs/{{glob}}*" >/dev/null; then \
       tail -n +1 {{workdir}}/local-logs/{{glob}}*; found=1; \
     fi; \
     [ "$found" = 1 ] || echo "no logs match {{glob}}"

# Matched against the run log, so it still reaches jobs whose recipe has since
# been renamed or deleted.

# scancel every still-live job matching GLOB.
cancel glob='*':
    @ids="$(python3 pipeline.py cancel-ids {{spec}} '{{glob}}' --workdir '{{workdir}}' | tr '\n' ' ')"; \
     if [ -z "${ids// /}" ]; then echo "no live jobs match {{glob}}"; else \
       {{scancel}} $ids && echo "cancelled: $ids"; \
     fi
