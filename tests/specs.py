"""Reusable spec fragments for the mock-harness tests. Kept small and focused;
each mirrors a real topology from the parent project's pipeline.toml."""

# A child array depending on two parent arrays, mirroring `testing-csk` ← (genquery,
# ib-core-decomp). `down` carries an extra `rep` axis, so its grid differs from each
# parent's -> whole-array `afterok` (not element-wise `aftercorr`), same as the real case.
FANIN = """
[recipe.up]
array   = true
params  = { dataset = ["a", "b"] }
command = "echo up ${dataset}"

[recipe.side]
array   = true
params  = { dataset = ["a", "b"] }
command = "echo side ${dataset}"

[recipe.down]
array   = true
deps    = ["up(dataset=${dataset})", "side(dataset=${dataset})"]
params  = { dataset = ["a", "b"], rep = ["0", "1", "2"] }
command = "echo down ${dataset} ${rep}"
slurm   = { cpus = 1 }
"""

# One recipe split into several arrays by `array_axes`, mirroring `testing-csk`
# (7 arrays of 60). Here: 2 arrays of 3, so `status` rolls the recipe up and the
# task histogram has to count across units rather than within one.
SPLIT = """
[recipe.wide]
array      = true
array_axes = ["rep"]
params     = { dataset = ["a", "b"], rep = ["0", "1", "2"] }
command    = "echo wide ${dataset} ${rep}"
"""
