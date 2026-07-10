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
CMDDIR="$1"

# Each task is its own script (task-<idx>.sh), so a task's command runs intact
# regardless of how many lines it spans.
SCRIPT="$CMDDIR/task-${SLURM_ARRAY_TASK_ID}.sh"
if [ ! -f "$SCRIPT" ]; then
  echo "no task script for array index $SLURM_ARRAY_TASK_ID in $CMDDIR" >&2
  exit 1
fi

# Run this task's script inside the container, exactly like run.sbatch.sh does
# for individual jobs.
exec apptainer exec "$CONTAINER" bash "$SCRIPT"
