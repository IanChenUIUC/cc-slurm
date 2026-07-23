set shell := ["bash", "-euo", "pipefail", "-c"]

# The spec and the two runners are variables so you can override per-invocation:
#   just run spec=other.toml
#   just local                       # uses cc-local instead of cc-submit
spec       := "pipeline.toml"
cc_submit  := "./cc-submit"
cc_local   := "bash ./cc-local"
sacct      := "ssh cc sacct"
workdir    := ".pipeline"
slurmlog   := "/scratch/ianchen3/slurm"

default:
    @just --list

# ---- inspection (no cluster, no side effects) ------------------------------

# Print the resolved DAG: nodes, edges, and dependency types.
dag spec=spec:
    python3 pipeline.py dag {{spec}}

# Print the cc-submit commands that WOULD run, and materialize the scripts
# into .pipeline/scripts/ for inspection (placeholder ids; nothing submitted).
dry spec=spec:
    python3 pipeline.py dry {{spec}}

# ---- running ---------------------------------------------------------------

# Reconcile, then submit failed/absent nodes (+ downstream) to SLURM.
# Optional GLOB restricts the run to matching nodes (their upstream must be done).
run glob='*' spec=spec:
    python3 pipeline.py submit {{spec}} --cc-submit '{{cc_submit}}' --sacct '{{sacct}}' --only '{{glob}}'

# Same, but run jobs locally in the container via cc-local (no SLURM).
# Synchronous: jobs are logged COMPLETED/FAILED directly, sacct is not consulted.
local glob='*' spec=spec:
    python3 pipeline.py submit {{spec}} --cc-submit '{{cc_local}}' --local --only '{{glob}}'

# Force-resubmit nodes matching GLOB this run only (transient), then submit.
rerun glob spec=spec:
    python3 pipeline.py submit {{spec}} --cc-submit '{{cc_submit}}' --sacct '{{sacct}}' --rerun '{{glob}}'

# Force-resubmit nodes matching GLOB, WITHOUT re-propagating to downstream nodes
# (upstream must already be COMPLETED). Like `rerun` but scoped to GLOB only.
force glob spec=spec:
    python3 pipeline.py submit {{spec}} --cc-submit '{{cc_submit}}' --sacct '{{sacct}}' --only '{{glob}}' --rerun '{{glob}}'

# ---- state -----------------------------------------------------------------

# Reconcile against sacct and print each node's state, elapsed, and peak RSS.
# Grouped array recipes roll up to one summary line; pass verbose=1 to expand them.
status spec=spec verbose='':
    python3 pipeline.py status {{spec}} --sacct '{{sacct}}' {{ if verbose == '' { '' } else { '-v' } }}

# Persistently mark nodes matching GLOB stale; next `run` reruns them + downstream.
invalidate glob spec=spec:
    python3 pipeline.py invalidate {{spec}} '{{glob}}'

# Force nodes matching GLOB to COMPLETED (e.g. after a manual re-run); `run` then
# skips them and won't re-propagate downstream. Undone by a later invalidate/rerun.
complete glob spec=spec:
    python3 pipeline.py complete {{spec}} '{{glob}}'

# ---- utilities -------------------------------------------------------------

# Tail the SLURM (remote) and local output logs for nodes matching GLOB.
# Remote logs are named slurm-<jobid>.out (arrays: slurm-<jobid>_<idx>.out), so
# we map GLOB -> job-id patterns via `log-ids` and tail them over ssh.
logs glob='*' spec=spec:
    @patterns="$(python3 pipeline.py log-ids {{spec}} '{{glob}}' | tr '\n' ' ')"; \
     found=0; \
     if [ -n "${patterns// /}" ]; then \
       ssh cc "cd {{slurmlog}} && tail -n +1 $patterns" && found=1 || true; \
     fi; \
     if compgen -G "{{workdir}}/local-logs/{{glob}}*" >/dev/null; then \
       tail -n +1 {{workdir}}/local-logs/{{glob}}*; found=1; \
     fi; \
     [ "$found" = 1 ] || echo "no logs match {{glob}}"

# scancel every still-live (non-terminal) job recorded in the run log.
cancel spec=spec:
    @python3 pipeline.py cancel-ids {{spec}} | xargs -r scancel && echo "cancelled live jobs"
