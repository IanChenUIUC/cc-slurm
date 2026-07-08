set shell := ["bash", "-euo", "pipefail", "-c"]

# The spec and the two runners are variables so you can override per-invocation:
#   just run spec=other.toml
#   just local                       # uses cc-local instead of cc-submit
spec       := "pipeline.toml"
cc_submit  := "./cc-submit"
cc_local   := "bash ./cc-local"
sacct      := "ssh cc sacct"
workdir    := ".pipeline"

default:
    @just --list

# ---- inspection (no cluster, no side effects) ------------------------------

# Print the resolved DAG: nodes, edges, and dependency types.
dag spec=spec:
    python3 pipeline.py dag {{spec}}

# Print the exact cc-submit commands that WOULD run (placeholder ids).
dry spec=spec:
    python3 pipeline.py dry {{spec}}

# ---- running ---------------------------------------------------------------

# Reconcile, then submit failed/absent nodes (+ downstream) to SLURM.
run spec=spec:
    python3 pipeline.py submit {{spec}} --cc-submit '{{cc_submit}}' --sacct '{{sacct}}'

# Same, but run jobs locally in the container via cc-local (no SLURM).
local spec=spec:
    python3 pipeline.py submit {{spec}} --cc-submit '{{cc_local}}' --sacct '{{sacct}}'

# Force-resubmit nodes matching GLOB this run only (transient), then submit.
rerun glob spec=spec:
    python3 pipeline.py submit {{spec}} --cc-submit '{{cc_submit}}' --sacct '{{sacct}}' --rerun '{{glob}}'

# ---- state -----------------------------------------------------------------

# Reconcile against sacct and print each node's state, elapsed, and peak RSS.
status spec=spec:
    python3 pipeline.py status {{spec}} --sacct '{{sacct}}'

# Persistently mark nodes matching GLOB stale; next `run` reruns them + downstream.
invalidate glob spec=spec:
    python3 pipeline.py invalidate {{spec}} '{{glob}}'

# ---- utilities -------------------------------------------------------------

# Tail the SLURM/local output logs for nodes whose file matches GLOB.
logs glob="*":
    @tail -n +1 {{workdir}}/local-logs/{{glob}}* 2>/dev/null || \
     tail -n +1 /u/ianchen3/scratch/slurm/{{glob}}* 2>/dev/null || \
     echo "no logs match {{glob}}"

# scancel every still-live (non-terminal) job recorded in the run log.
cancel spec=spec:
    @python3 pipeline.py cancel-ids {{spec}} | xargs -r scancel && echo "cancelled live jobs"
