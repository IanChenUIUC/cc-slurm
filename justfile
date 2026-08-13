set shell := ["bash", "-euo", "pipefail", "-c"]

# Every verb takes an optional GLOB matching node identities, e.g. `just run 'testing-*'`.
#
# Behaviour flags are `just` variable overrides, so they go BEFORE the verb:
#
#   just dry=1     run 'testing-*'      # what it would submit, and why: same decisions,
#                                       #   printed instead of issued
#   just local=1   run 'testing-*'      # here, synchronously, via cc-local (no SLURM)
#   just force=1   run 'testing-*'      # the inputs or the code changed: redo these
#                                       #   AND everything downstream. Sugar for
#                                       #   `just invalidate` followed by `just run`
#   just force=1 down=0 run 'testing-*' # the job flaked: redo these alone and leave
#                                       #   downstream results in place
#   just up=1      run 'testing-*'      # these, plus whatever upstream they still need
#   just retry=0   run                  # only what has never been attempted: leave every
#                                       #   past failure, and its downstream, alone
#   just verbose=1 status               # expand rolled-up recipes and name failed tasks
#   just spec=other.toml dag            # any variable below can be overridden this way
#
# (`just` only accepts overrides ahead of the recipe name; anything after it is a
# positional argument, so `just run 'g' local=1` would be read as the spec path.)

local   := ""
force   := ""
up      := ""
down    := "1"
retry   := "1"
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

# The flags are variables, not recipes, so `just --list` cannot show them; a bare
# `just` prints them instead.

# Print the behaviour flags (bare `just` does this).
default: help

# Print the behaviour flags, which go BEFORE the verb.
help:
    @echo 'flags (go BEFORE the verb):'
    @echo '  local=1    run here, synchronously, no SLURM'
    @echo '  force=1    redo these and everything downstream'
    @echo '  down=0     with force: these alone, leave downstream results in place'
    @echo '  up=1       also run whatever upstream they need'
    @echo '  dry=1      print the decisions instead of issuing them'
    @echo '  retry=0    skip anything already attempted and failed'
    @echo '  verbose=1  expand rolled-up rows to per-unit ones'
    @echo
    @echo 'verbs: just --list'

# ---- observing: what the DAG is, and what happened to it ------------------

# The resolved DAG: one line per recipe, with its edges.
dag glob='*':
    python3 pipeline.py dag {{spec}} '{{glob}}' {{ if verbose == '' { '' } else { '-v' } }}

# Per recipe: elapsed, peak RSS, and a task histogram.
status glob='*':
    python3 pipeline.py status {{spec}} '{{glob}}' --workdir '{{workdir}}' --sacct '{{sacct}}' \
        {{ if verbose == '' { '' } else { '-v' } }} {{ if local == '' { '' } else { '--local' } }}

# Every logged attempt per unit: when, what happened, job id, state, RSS.
history glob='*':
    python3 pipeline.py history {{spec}} '{{glob}}' --workdir '{{workdir}}'

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

# ---- running ---------------------------------------------------------------

# See the flag notes at the top of this file for local / force / down / up / dry.
# `dry=1` is the one to reach for before any run you're unsure about: printed
# instead of issued.

# Submit whatever isn't already COMPLETED, plus anything downstream of it.
run glob='*':
    python3 pipeline.py submit {{spec}} --workdir '{{workdir}}' {{runner}} \
        {{ if dry == '' { '' } else { '--dry' } }} {{ if up == '' { '' } else { '--deps' } }} \
        {{ if retry == '0' { '--no-retry' } else { '' } }} \
        {{ if force == '' { "--only '" + glob + "'" } \
           else { if down == '0' { "--only '" + glob + "' --rerun '" + glob + "'" } \
                  else { "--rerun '" + glob + "'" } } }}

# ---- state -----------------------------------------------------------------

# Mark units stale; the next `run` redoes them and their downstream.
invalidate glob:
    python3 pipeline.py invalidate {{spec}} '{{glob}}' --workdir '{{workdir}}' \
        {{ if force == '' { '' } else { '--force' } }} \
        {{ if dry == '' { '' } else { '--dry' } }}

# Force units to COMPLETED.
complete glob:
    python3 pipeline.py complete {{spec}} '{{glob}}' --workdir '{{workdir}}' \
        {{ if dry == '' { '' } else { '--dry' } }}

# Mark units CANCELLED and scancel whatever is live.
cancel glob='*':
    @ids="$(python3 pipeline.py cancelled {{spec}} '{{glob}}' --workdir '{{workdir}}' \
              --print-ids {{ if dry == '' { '' } else { '--dry' } }} | tr '\n' ' ')"; \
     if [ -z "${ids// /}" ]; then echo "no live jobs match {{glob}}"; \
     elif [ -n '{{dry}}' ]; then echo "(dry) would scancel: $ids"; \
     else {{scancel}} $ids && echo "scancelled: $ids"; fi
