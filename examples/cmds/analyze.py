"""Body for [recipe.analyze], interpolated by the engine before it is materialized.

Every interpolation is assigned to a module-level constant here rather than inlined
into an expression: substitution is textual, so a value has to land inside a string
literal to survive. A list interpolates space-joined, hence the .split().

Note there is no escape syntax, so a placeholder-looking literal anywhere in this
file -- including in a comment or docstring -- is interpolated too, and fails as an
undefined variable. Build such a string at runtime instead.
"""
import pathlib

RAW = "${ingest.raw}"
STATS = "${stats}"
DATASET = "${dataset}"
REP = int("${rep}")
CPUS = int("${slurm.cpus}")
REPS = [int(r) for r in "${reps}".split()]      # the recipe's own alias, unqualified

values = [int(x) for x in pathlib.Path(RAW).read_text().split()]
total = sum(v + REP for v in values)

pathlib.Path(STATS).write_text(
    f"dataset={DATASET} rep={REP}/{len(REPS)} cpus={CPUS} total={total}\n"
)
print(f"analyzed {DATASET} rep {REP}: total={total}")
