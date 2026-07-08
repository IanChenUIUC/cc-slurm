#!/bin/bash
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --partition=secondary
#SBATCH --mem=64GB
#SBATCH --output=/u/ianchen3/scratch/slurm/slurm-%A_%a.out

set -euo pipefail

CONTAINER=/u/ianchen3/venv/python_bootstrap-sandbox
CMDFILE="$1"

# Pick this task's line (0-indexed array -> 1-indexed sed).
LINE=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$CMDFILE")
if [ -z "$LINE" ]; then
  echo "no command at array index $SLURM_ARRAY_TASK_ID in $CMDFILE" >&2
  exit 1
fi

# Run the (already bash -c wrapped) command line inside the container.
exec apptainer exec "$CONTAINER" bash -c "$LINE"
