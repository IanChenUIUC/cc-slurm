"""spec.md §6 deps / captures. (Dashed-recipe case migrated from the original
test_multiline_array.py.)"""

DASHED = """\
[recipe.gbbs-format]
params  = { dataset = ["abm14", "abm15"] }
command = "./format ${dataset} > ${output}"
output  = "out/${dataset}.gbbs"

[recipe.run-bench]
params  = { dataset = ["abm14", "abm15"] }
deps    = ["gbbs-format(dataset=${dataset})"]
command = "./bench ${gbbs-format.output}"
"""


def test_dashed_recipe_captures_parse_and_wire(mock_run):
    """Recipe names are TOML bare keys and may contain '-'. A capture naming a
    dashed recipe (`gbbs-format(dataset=abm14)`) must parse and wire, not raise
    `malformed capture`."""
    r = mock_run(DASHED, {}, "dag")
    assert r.ok, r.stderr
    assert "malformed capture" not in (r.stdout + r.stderr)
    assert "gbbs-format" in r.stdout and "run-bench" in r.stdout
