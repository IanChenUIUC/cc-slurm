#!/bin/bash
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --partition=secondary
#SBATCH --mem=64GB
#SBATCH --output=/scratch/ianchen3/slurm/slurm-%A.out

set -euo pipefail

CONTAINER=/u/ianchen3/venv/python_bootstrap-sandbox
SCRIPT="$1"; shift

exec apptainer exec "$CONTAINER" bash "$SCRIPT" "$@"
